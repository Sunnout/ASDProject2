package protocols.agreement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.*;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.requests.SameReplicasRequest;
import protocols.agreement.timers.MembershipOkTimer;
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
    private int joinedInstance; // Instance in which we joined the system
    private Map<Integer, PaxosState> paxosByInstance; // PaxosState for each instance


    public PaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        currentInstance = -1; // -1 means we have not yet joined the system
        joinedInstance = -1; //-1 means we have not yet joined the system
        paxosByInstance = new HashMap<>();

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(PaxosTimer.TIMER_ID, this::uponPaxosTimer);
        registerTimerHandler(MembershipOkTimer.TIMER_ID, this::uponMembershipOkTimer);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplicaRequest);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplicaRequest);
        registerRequestHandler(SameReplicasRequest.REQUEST_ID, this::uponSameReplicasRequest);


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
        registerMessageSerializer(cId, DecidedMessage.MSG_ID, DecidedMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOkMessage.MSG_ID, this::uponPrepareOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, DecidedMessage.MSG_ID, this::uponDecidedMessage, this::uponMsgFail);

        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }


    /*--------------------------------- Messages ---------------------------------------- */


    private void uponDecidedMessage(DecidedMessage msg, Host host, short sourceProto, int channelId) {
        try {
            OperationAndId opnId = new OperationAndId(Operation.fromByteArray(msg.getOp()), msg.getOpId());
            int instance = msg.getInstance();
            PaxosState ps = getPaxosInstance(currentInstance);

            if (currentInstance == instance && ps.isMembershipOk()) {
                logger.debug("Decided {} in instance {}", opnId.getOpId(), instance);
                triggerNotification(new DecidedNotification(instance, opnId.getOpId(),
                        opnId.getOperation().toByteArray()));
                cancelTimer(ps.getPaxosTimer());

                currentInstance++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();
            int msgSn = msg.getSn();
            PaxosState ps = getPaxosInstance(instance);
            // If the message is not from an instance that has already ended
            if (instance >= currentInstance) {
                logger.debug("Received prepare with sn {} in instance {}", msgSn, instance);

                // If seqNumber of prepare is higher than our highest prepare, send prepareOk
                if (msgSn > ps.getHighestPrepare()) {
                    ps.setHighestPrepare(msgSn);

                    // If we are not joined, respond only to the sender
                    if (currentInstance == -1)
                        openConnection(host);

                    int highestAccepted = ps.getHighestAccept();
                    // If we have accepted something, send highest accepted seqNumber and value
                    if (highestAccepted != -1) {
                        OperationAndId opToSend = ps.getHighestAcceptedValue();
                        logger.debug("Have accepted {} in instance {}: sending it", opToSend.getOpId(), instance);
                        sendMessage(new PrepareOkMessage(instance, opToSend.getOpId(),
                                opToSend.getOperation().toByteArray(), highestAccepted, msgSn), host);
                    }
                    // If we have not accepted anything, send bottoms (nulls and -1)
                    else {
                        logger.debug("Have accepted nothing in instance {}: sending bottoms", instance);
                        sendMessage(new PrepareOkMessage(instance, null, null, -1, msgSn), host);
                    }
                }
            } else {
                logger.debug("Sending previous decision for instance {} currentInstance {} To Host {}"
                        , instance, currentInstance, host);
                OperationAndId opnId = ps.getHighestLearnedValue();
                sendMessage(new DecidedMessage(instance, opnId.getOpId(),
                        opnId.getOperation().toByteArray()), host);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponPrepareOkMessage(PrepareOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();
            PaxosState ps = getPaxosInstance(instance);

            // If the message is not from an instance that has already ended
            // and we don't have a majority of prepareOks
            if (instance >= currentInstance && !ps.havePrepareOkMajority()) {
                int highestAccepted = msg.getHighestAccepted();
                int msgSn = msg.getSn();
                logger.debug("Received prepareOk for {} in instance {}", msgSn, instance);

                // If the seqNumber of prepareOk is higher than the one we have and
                // the accepted sn is not bottom, we replace the value with the new one
                if (msgSn > ps.getHighestPrepareOk()) {
                    // Reset counter because we changed seqNumber
                    logger.debug("Reset prepare ok from {} to {}", ps.getHighestPrepareOk(), msgSn);
                    ps.resetPrepareOkCounter();
                    // Increment counter of prepareOks
                    ps.incrementPrepareOkCounter();
                    logger.debug("Incremented prepareOk counter to {} in instance {}", ps.getPrepareOkCounter(), instance);
                    ps.setHighestPrepareOk(msgSn);

                    if (highestAccepted != -1) {
                        ps.setMaxSnAccept(highestAccepted);
                        ps.setHighestAcceptedValue(new OperationAndId(Operation.fromByteArray(msg.getOp())
                                , msg.getOpId()));
                    }

                }
                // Else if the seqNumber of prepareOk is equal to the one we have and
                // the accepted sn is not bottom and is higher than the one we have,
                // we replace the value with the new one
                else if (msgSn == ps.getHighestPrepareOk()) {
                    // Reset counter because we changed seqNumber
                    logger.debug("Same prepare ok: {}", msgSn);
                    // Increment counter of prepareOks
                    ps.incrementPrepareOkCounter();
                    logger.debug("Incremented prepareOk counter to {} in instance {}"
                            , ps.getPrepareOkCounter(), instance);
                    if (highestAccepted > ps.getMaxSnAccept()) {
                        ps.setMaxSnAccept(highestAccepted);
                        ps.setHighestAcceptedValue(new OperationAndId(Operation.fromByteArray(msg.getOp())
                                , msg.getOpId()));
                    }
                }

                // If majority quorum was achieved
                if (ps.getPrepareOkCounter() >= ps.getQuorumSize()) {
                    logger.debug("Got PrepareOk majority for sn {} in instance {}", ps.getHighestPrepareOk(), instance);
                    ps.setPrepareOkMajority(true);

                    // If highest accepted value is null, then our seqNumber was the winner
                    // and we choose our initial proposed value
                    if (ps.getHighestAcceptedValue() == null) {
                        ps.setHighestAcceptedValue(ps.getInitialProposal());
                        logger.debug("Going to send accepts for my proposal in instance {}", instance);
                    }

                    OperationAndId opnId = ps.getHighestAcceptedValue();
                    // Send accept messages to all
                    for (Host h : ps.getMembership()) {
                        sendMessage(new AcceptMessage(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray(), ps.getSn()), h);
                    }
                    logger.debug("Sent AcceptMessages for op with sn {} in instance {}", ps.getSn(), instance);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();

            int msgSn = msg.getSn();
            PaxosState ps = getPaxosInstance(instance);
            // If the message is not from an instance that has already ended
            if (instance >= currentInstance) {
                logger.debug("Received accept with sn {} in instance {}", msgSn, instance);

                // If seqNumber of accept is equal or higher than our highest prepare
                if (msgSn >= ps.getHighestPrepare()) {
                    logger.debug("Can accept op with sn {} in instance {}", msgSn, instance);
                    ps.setHighestAccept(msgSn);
                    OperationAndId opnId = new OperationAndId(Operation.fromByteArray(msg.getOp()),
                            msg.getOpId());
                    ps.setHighestAcceptedValue(opnId);


                    // If we are not joined, respond only to the sender
                    if (currentInstance == -1) {
                        logger.debug("Sent AcceptOkMessage for {}", host);
                        openConnection(host);
                        sendMessage(new AcceptOkMessage(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray(), msgSn), host);
                    } else {
                        logger.debug("Sent AcceptOkMessages for {} in instance {}", opnId.getOpId(), instance);
                        // Send acceptOk with that seqNumber and value to all learners
                        for (Host h : ps.getMembership()) {
                            sendMessage(new AcceptOkMessage(instance, opnId.getOpId(),
                                    opnId.getOperation().toByteArray(), msgSn), h);
                        }
                    }
                }
            } else {
                logger.debug("Sending previous decision for instance {}", instance);
                OperationAndId opnId = ps.getHighestLearnedValue();
                sendMessage(new DecidedMessage(instance, opnId.getOpId(),
                        opnId.getOperation().toByteArray()), host);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponAcceptOkMessage(AcceptOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int instance = msg.getInstance();
            PaxosState ps = getPaxosInstance(instance);

            // If the message is not from an instance that has already ended
            // and we don't have a majority of acceptOks
            if (instance >= currentInstance) {
                logger.debug("Received AcceptOk with sn {} in instance {}", msg.getHighestAccept(), instance);

                int highestLearned = ps.getHighestLearned();
                int msgHighestAccept = msg.getHighestAccept();

                // Update learned value and reset counter if the seqNumber is higher
                if (msgHighestAccept > highestLearned) {
                    logger.debug("Received Higher AcceptOk, changing from {} to {}", highestLearned, msgHighestAccept);
                    ps.resetHaveAccepted();
                    ps.addHostToHaveAccepted(host);
                    ps.setHighestLearned(msgHighestAccept);
                    ps.setHighestLearnedValue(new OperationAndId(Operation.fromByteArray(msg.getOp()),
                            msg.getOpId()));

                } else if (msgHighestAccept == highestLearned) {
                    logger.debug("Increment AcceptOk Counter in instance {}", instance);
                    ps.addHostToHaveAccepted(host);
                }

                // If majority quorum was achieved
                logger.debug("Checking Quorum with Membership: {} in instance {}", ps.getMembership(), instance);
                if (ps.hasAcceptOkQuorum() && ps.getToDecide() == null) {
                    logger.debug("List size: {}", ps.getMembership().size());
                    logger.debug("Got AcceptOk majority for instance {}", instance);

                    // Cancel PaxosTimer for this instance
                    logger.debug("Cancelled PaxosTimer in instance {}", instance);
                    cancelTimer(ps.getPaxosTimer());

                    OperationAndId opnId = ps.getHighestLearnedValue();
                    // If the quorum is for the current instance then decide
                    ps.setToDecide(opnId);
                    if (currentInstance == instance) {
                        logger.debug("Decided {} in instance {}", opnId.getOpId(), instance);
                        triggerNotification(new DecidedNotification(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray()));

                        currentInstance++;
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
            joinedInstance = currentInstance;
            PaxosState ps = getPaxosInstance(currentInstance);

            // Initialize membership and open connections
            for (Host h : notification.getMembership()) {
                ps.addReplicaToMembership(h);
                openConnection(h);
            }
            ps.setMembershipOk();

            logger.info("Agreement starting at instance {},  membership: {}",
                    currentInstance, ps.getMembership());

            // If we decided a value before joining, send acceptOk and trigger decide

            canDecide(currentInstance);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        try {
            int instance = request.getInstance();
            logger.debug("New Propose for instance {}", instance);
            PaxosState ps = getPaxosInstance(instance);
            // Save proposed value
            ps.setInitialProposal(new OperationAndId(Operation.fromByteArray(request.getOperation()),
                    request.getOpId()));

            sendPrepare(instance);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponAddReplicaRequest(AddReplicaRequest request, short sourceProto) {
        logger.debug("Add replica {} in instance {}", request.getReplica(), request.getInstance());
        Host replica = request.getReplica();
        int instance = request.getInstance();
        PaxosState ps = getPaxosInstance(instance);
        // Adding replica to membership of instance and opening connection
        ps.addReplicaToMembership(replica);
        openConnection(replica);
        // Membership up to date
        usePreviousMembership(instance);
        ps.setMembershipOk();
        // If we decided a value before joining, send acceptOk and trigger decide
        canDecide(instance);
    }

    private void uponRemoveReplicaRequest(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Remove replica {} in instance {}", request.getReplica(), request.getInstance());
        Host replica = request.getReplica();
        int instance = request.getInstance();
        PaxosState ps = getPaxosInstance(request.getInstance());
        // Removing replica from membership of instance and closing connection
        ps.removeReplicaFromMembership(replica);
        closeConnection(replica);
        // Membership up to date
        usePreviousMembership(instance);
        ps.setMembershipOk();
        // If we decided a value before joining, send acceptOk and trigger decide
        canDecide(instance);
    }

    private void uponSameReplicasRequest(SameReplicasRequest request, short sourceProto) {
        logger.debug("No changes to membership in instance {}", request.getInstance());
        int instance = request.getInstance();
        PaxosState ps = getPaxosInstance(instance);
        // Membership up to date
        usePreviousMembership(instance);
        ps.setMembershipOk();
        // If we decided a value before joining, send acceptOk and trigger decide
        canDecide(instance);
    }

    /*--------------------------------- Timers ---------------------------------------- */

    private void uponPaxosTimer(PaxosTimer paxosTimer, long timerId) {
        int instance = paxosTimer.getInstance();
        PaxosState ps = getPaxosInstance(instance);

        // If we have not decided yet, retry prepare
        if (ps.getToDecide() == null) {
            logger.debug("PaxosTimer Timeout in instance {}", instance);
            // Increasing seqNumber
            ps.increaseSn();
            ps.resetPrepareOkCounter();
            ps.setPrepareOkMajority(false);
            ps.resetHaveAccepted();
            List<Host> membership = ps.getMembership();
            membership.forEach(h -> sendMessage(new PrepareMessage(ps.getSn(), instance), h));
            logger.debug("Retry sending to: {} in instance {}", membership, instance);

            // Setup new PaxosTimer that expires if we don't decide
            long newTimerId = setupTimer(new PaxosTimer(instance), 2000);
            ps.setPaxosTimer(newTimerId);
            logger.debug("uponPaxosTimer: New PaxosTimer created with id {}", newTimerId);
        }
    }

    private void uponMembershipOkTimer(MembershipOkTimer timer, long timerId) {
        sendPrepare(timer.getInstance());
    }

    /*--------------------------------- Procedures ---------------------------------------- */

    private PaxosState getPaxosInstance(int instance) {
        // If we haven't initialized the instance
        if (!paxosByInstance.containsKey(instance))
            paxosByInstance.put(instance, new PaxosState());

        return paxosByInstance.get(instance);
    }

    private void sendPrepare(int instance) {
        PaxosState ps = getPaxosInstance(instance);
        if (ps.isMembershipOk()) {
            // Generating seqNumber
            ps.generateSn(myself);

            // Send prepares to every node in membership
            List<Host> membership = ps.getMembership();
            membership.forEach(h -> sendMessage(new PrepareMessage(ps.getSn(), instance), h));
            logger.debug("Sent Prepares for instance {}", instance);

            // Setup PaxosTimer that expires if we don't decide
            long timerId = setupTimer(new PaxosTimer(instance), 2000);
            ps.setPaxosTimer(timerId);
            logger.debug("New PaxosTimer for instance {}", instance);

        } else {
            // Setup MembershipTimer that expires if we don't have membership
            logger.debug("New MembershipOkTimer for instance {}", instance);
            ps.setPaxosTimer(setupTimer(new MembershipOkTimer(instance), 50));
        }
    }

    private void usePreviousMembership(int instance) {
        PaxosState ps = getPaxosInstance(instance);
        logger.debug("Getting previous membership for instance {}", instance);

        if (joinedInstance != currentInstance) {
            List<Host> prevMembership = getPaxosInstance(instance - 1).getMembership();
            logger.debug("Previous Membership {}", prevMembership);
            for (Host h : prevMembership)
                ps.addReplicaToMembership(h);
        }

        logger.debug("New membership: {}", ps.getMembership());
    }

    private void canDecide(int instance) {
        try {
            PaxosState ps = getPaxosInstance(instance);
            if (ps.getNumberOfAcceptOks() >= ps.getQuorumSize()) {
                OperationAndId opnId = ps.getHighestLearnedValue();
                ps.setToDecide(opnId);
                
                for (Host h : ps.getMembership()) {
                    sendMessage(new AcceptOkMessage(instance, opnId.getOpId(),
                            opnId.getOperation().toByteArray(), ps.getHighestAccept()), h);
                }

                triggerNotification(new DecidedNotification(currentInstance++,
                        opnId.getOpId(), opnId.getOperation().toByteArray()));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}