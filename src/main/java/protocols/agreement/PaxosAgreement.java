package protocols.agreement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.*;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.timers.PaxosTimer;
import protocols.agreement.utils.PaxosState;
import protocols.app.utils.Operation;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.utils.OperationAndId;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class PaxosAgreement extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PaxosAgreement.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 400;
    public final static String PROTOCOL_NAME = "PaxosAgreement";

    private Host myself;
    private int currentInstance; // Instance that is currently running
    private Map<Integer, PaxosState> paxosByInstance; // PaxosState for each instance


    public PaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        currentInstance = -1; // -1 means we have not yet joined the system
        paxosByInstance = new HashMap<>();

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(PaxosTimer.TIMER_ID, this::uponPaxosTimer);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplicaRequest);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplicaRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for events from the application or agreement
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOkMessage.MSG_ID, PrepareOkMessage.serializer);
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOkMessage.MSG_ID, AcceptOkMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOkMessage.MSG_ID, this::uponPrepareOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage, this::uponMsgFail);

        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }


    /*--------------------------------- Messages ---------------------------------------- */

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();

            if (instance >= currentInstance) {
                int msgSn = msg.getSn();
                PaxosState ps = getPaxosInstance(instance);
                logger.debug("uponPrepareMessage: MsgSn: {}, MsgInstance: {}", msgSn, instance);

                // If seqNumber of prepare is higher than our highest prepare, send prepareOk
                if (msgSn > ps.getHighestPrepare()) {
                    ps.setHighestPrepare(msgSn);

                    int highestAccepted = ps.getHighestAccept();
                    // If we have accepted something, send highest accepted seqNumber and value
                    if (highestAccepted != -1) {
                        OperationAndId opToSend = ps.getHighestAcceptedValue();
                        logger.debug("uponPrepareMessage: sending OpId: {}", opToSend.getOpId());
                        sendMessage(new PrepareOkMessage(instance, opToSend.getOpId(),
                                opToSend.getOperation().toByteArray(), highestAccepted), host);
                    }
                    // If we have not accepted anything, send bottoms (nulls and -1)
                    else {
                        logger.debug("uponPrepareMessage: sending bottoms");
                        sendMessage(new PrepareOkMessage(instance, null, null, -1), host);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponPrepareOkMessage(PrepareOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();
            PaxosState ps = getPaxosInstance(instance);

            if (instance >= currentInstance && !ps.havePrepareOkMajority()) {
                int msgSn = msg.getHighestAccepted();

                logger.debug("uponPrepareOkMessage: MsgSn: {}, MsgInstance: {}", msgSn, instance);

                // If the prepareOk didn't return bottoms and the seqNumber is higher than the one
                // we have, we replace the value with the new one
                if (msgSn != -1) {
                    if (msgSn > ps.getHighestPrepareOk()) {
                        // Reset counter because we changed seqNumber
                        logger.debug("uponPrepareOkMessage: Old Sn: {}, New Sn: {}"
                                , ps.getHighestPrepareOk(), msgSn);

                        ps.resetPrepareOkCounter();
                        ps.setHighestPrepareOk(msgSn);
                        ps.setHighestAcceptedValue(new OperationAndId(Operation.fromByteArray(msg.getOp()),
                                msg.getOpId()));
                    }

                    // Increment counter of prepareOks
                    ps.incrementPrepareOkCounter();

                } else {
                    // If we have not received anything but bottoms
                    if (ps.getHighestPrepareOk() == -1) {
                        logger.debug("uponPrepareOkMessage: Another Bottom");
                        // Increment counter of prepareOks
                        ps.incrementPrepareOkCounter();
                    }
                }

                // If majority quorum was achieved send accept messages to all
                if (ps.getPrepareOkCounter() >= ps.getQuorumSize()) {
                    logger.debug("uponPrepareOkMessage: Got PrepareOk majority");
                    ps.setPrepareOkMajority(true);

                    // If highest prepare is -1, then our seqNumber was the winner
                    if (ps.getHighestPrepareOk() == -1) {
                        ps.setHighestAcceptedValue(ps.getInitialProposal());
                        logger.debug("uponPrepareOkMessage: Sending my Proposal");
                    }

                    OperationAndId opnId = ps.getHighestAcceptedValue();

                    for (Host h : ps.getMembership()) {
                        sendMessage(new AcceptMessage(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray(), ps.getSn()), h);
                    }

                    logger.debug("uponPrepareOkMessage: Sent AcceptMessages");
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();

            if (instance >= currentInstance) {
                int msgSn = msg.getSn();

                PaxosState ps = getPaxosInstance(instance);
                logger.debug("uponAcceptMessage: MsgSn: {}, MsgInstance: {}", msgSn, instance);

                // If seqNumber of accept is equal or higher than our highest prepare
                if (msgSn >= ps.getHighestPrepare()) {
                    logger.debug("uponAcceptMessage: Will accept this operation");
                    ps.setHighestAccept(msgSn);
                    OperationAndId opnId = new OperationAndId(Operation.fromByteArray(msg.getOp()),
                            msg.getOpId());
                    ps.setHighestAcceptedValue(opnId);

                    // Send acceptOk with that seqNumber and value to all learners
                    for (Host h : ps.getMembership()) {
                        sendMessage(new AcceptOkMessage(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray(), msgSn), h);
                    }

                    logger.debug("uponAcceptMessage: Sent AcceptOkMessages");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponAcceptOkMessage(AcceptOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();
            PaxosState ps = getPaxosInstance(instance);

            // To prevent from deciding more that once in the same instance
            if (instance >= currentInstance && !ps.haveAcceptOkMajority()) {
                logger.debug("uponAcceptOkMessage: MsgSn: {}, MsgInstance: {}", msg.getHighestAccept(), instance);

                // Update value and reset counter if the seqNumber is higher
                int highestLearned = ps.getHighestLearned();
                int msgHighestAccept = msg.getHighestAccept();

                if (msgHighestAccept > highestLearned) {
                    logger.debug("uponAcceptOkMessage: Old Sn: {}, New Sn: {}", highestLearned, msgHighestAccept);
                    ps.resetAcceptOkCounter();
                    ps.incrementAcceptOkCounter();
                    ps.setHighestLearned(msgHighestAccept);
                    ps.setHighestLearnedValue(new OperationAndId(Operation.fromByteArray(msg.getOp()),
                            msg.getOpId()));

                } else if (msgHighestAccept == highestLearned) {
                    logger.debug("uponAcceptOkMessage: Increment AcceptOk Counter");
                    ps.incrementAcceptOkCounter();
                }

                // If majority quorum was achieved
                if (ps.getAcceptOkCounter() >= ps.getQuorumSize()) {
                    logger.debug("uponAcceptOkMessage: Got AcceptOk majority");
                    ps.setAcceptOkMajority(true);

                    logger.debug("uponAcceptOkMessage: cancelled PaxosTimer {}", ps.getPaxosTimer());
                    cancelTimer(ps.getPaxosTimer());

                    OperationAndId opnId = ps.getHighestLearnedValue();
                    // If the quorum is for the current instance then decide
                    if (currentInstance == instance) {
                        triggerNotification(new DecidedNotification(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray()));

                        logger.debug("uponAcceptOkMessage: Decided {} in instance {}", opnId.getOpId(), instance);

                        currentInstance++;
                        // TODO: limpar tudo desta instancia pq já acabou??
                        // Execute all pending decisions, if there are any
                        ps = getPaxosInstance(currentInstance);

                        opnId = ps.getToDecide();
                        while (opnId != null) {
                            logger.debug("uponAcceptOkMessage: Decided {} in instance {}",
                                    opnId.getOpId(), currentInstance);
                            triggerNotification(new DecidedNotification(currentInstance++,
                                    opnId.getOpId(), opnId.getOperation().toByteArray()));
                            // TODO: limpar tudo destas instancias pq já acabaram??
                            ps = getPaxosInstance(currentInstance);
                            opnId = ps.getToDecide();
                        }
                    }
                    // If the quorum is not for the current instance save decision for later
                    else {
                        ps.setToDecide(opnId);
                        logger.debug("uponAcceptOkMessage: Saved Operation for future decide");
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        // If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }


    /*--------------------------------- Notifications ------------------------------------ */

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        try {
            // We joined the system and can now start doing things
            currentInstance = notification.getJoinInstance();
            PaxosState ps = getPaxosInstance(currentInstance);

            logger.info("Agreement starting at instance {},  membership: {}",
                    currentInstance, ps.getMembership());

            for (Host h : notification.getMembership()) {
                ps.addReplicaToMembership(h);
                openConnection(h);
            }

            // If we already decided a value before joining, trigger the decide
            OperationAndId opnId = ps.getToDecide();
            while (opnId != null) {
                triggerNotification(new DecidedNotification(currentInstance++,
                        opnId.getOpId(), opnId.getOperation().toByteArray()));
                ps = getPaxosInstance(currentInstance);
                opnId = ps.getToDecide();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        try {
            logger.debug("uponProposeRequest: New Propose");

            int instance = request.getInstance();
            PaxosState ps = getPaxosInstance(instance);
            // Generating seqNumber
            ps.generateSn(myself);
            List<Host> membership = ps.getMembership();
            membership.forEach(h -> sendMessage(new PrepareMessage(ps.getSn(), instance), h));
            logger.debug("uponProposeRequest: Sent Prepares");

            // Save proposed value
            ps.setInitialProposal(new OperationAndId(Operation.fromByteArray(request.getOperation()),
                    request.getOpId()));

            long timerId = setupTimer(new PaxosTimer(instance), 5000);
            logger.debug("uponProposeRequest: New PaxosTimer created with id {}", timerId);
            ps.setPaxosTimer(timerId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponAddReplicaRequest(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        int replicaInstance = request.getInstance();
        Host replica = request.getReplica();
        PaxosState ps = getPaxosInstance(replicaInstance);
        // Adding replica to membership of instance
        ps.addReplicaToMembership(replica);
        openConnection(replica);
    }

    private void uponRemoveReplicaRequest(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        int replicaInstance = request.getInstance();
        Host replica = request.getReplica();
        PaxosState ps = getPaxosInstance(replicaInstance);
        // Removing replica from membership of instance
        ps.removeReplicaFromMembership(replica);
        closeConnection(replica);
    }

    /*--------------------------------- Timers ---------------------------------------- */

    private void uponPaxosTimer(PaxosTimer paxosTimer, long timerId) {
        logger.debug("PaxosTimer Timeout with id: {}", timerId);
        int instance = paxosTimer.getInstance();
        PaxosState ps = getPaxosInstance(instance);

        // If we have not decided yet, retry
        if (!ps.haveAcceptOkMajority()) {
            // Increasing seqNumber
            ps.increaseSn();
            ps.setPrepareOkMajority(false);
            ps.setAcceptOkMajority(false);
            List<Host> membership = ps.getMembership();
            membership.forEach(h -> sendMessage(new PrepareMessage(ps.getSn(), instance), h));
            logger.debug("uponPaxosTimer: Retry sending to: " + membership);
        }
    }

    /*--------------------------------- Procedures ---------------------------------------- */

    private PaxosState getPaxosInstance(int instance) {
        if (!paxosByInstance.containsKey(instance)) {
            PaxosState newPaxos = new PaxosState();

            // Get membership from last instance
            if (instance > 0) {
                List<Host> previousMembership = paxosByInstance.get(instance - 1).getMembership();

                for (Host h : previousMembership) {
                    newPaxos.addReplicaToMembership(h);

                    if(currentInstance == -1)
                        openConnection(h);
                }
            }

            paxosByInstance.put(instance, newPaxos);
        }
        return paxosByInstance.get(instance);
    }
}