package protocols.agreement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.*;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.timers.AcceptOkTimer;
import protocols.agreement.timers.PrepareOkTimer;
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
        registerTimerHandler(PrepareOkTimer.TIMER_ID, this::uponPrepareOkTimer);
        registerTimerHandler(AcceptOkTimer.TIMER_ID, this::uponAcceptOkTimer);

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
        //logger.debug("Entrei no Upon Prepare");
        try {
            int msgSn = msg.getSn();
            int instance = msg.getInstance();
            PaxosState ps = getPaxosInstance(instance);

            // If seqNumber of prepare is higher than our highest prepare, send prepareOk
            if (msgSn > ps.getHighestPrepare()) {
                //logger.debug("Entrei no if");
                ps.setHighestPrepare(msgSn);

                int highestAccepted = ps.getHighestAccept();
                // If we have accepted something, send highest accepted seqNumber and value
                if (highestAccepted != -1) {
                    //logger.debug("Accepted something before");
                    OperationAndId opToSend = ps.getHighestAcceptedValue();
                    sendMessage(new PrepareOkMessage(instance, opToSend.getOpId(),
                            opToSend.getOperation().toByteArray(), highestAccepted), host);
                }
                // If we have not accepted anything, send bottoms (nulls and -1)
                else {
                    //logger.debug("Nothing Accepted");
                    sendMessage(new PrepareOkMessage(instance, null, null, -1), host);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponPrepareOkMessage(PrepareOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            //logger.debug("Prepare OK Received");

            if (currentInstance >= 0) {
                //logger.debug("Current >= 0");

                int msgSn = msg.getHighestAccepted();
                int instance = msg.getInstance();
                PaxosState ps = getPaxosInstance(instance);

                // If the prepareOk didn't return bottoms and the seqNumber is higher than the one
                // we have, we replace the value with the new one
                if (msgSn != -1) {
                    //logger.debug("MSG != -1");
                    if (msgSn > ps.getHighestPrepareOk()) {
                        //logger.debug("Reset");
                        // Reset counter because we changed seqNumber
                        ps.resetPrepareOkCounter();
                        ps.setHighestPrepareOk(msgSn);
                        // TODO: mudei para accepted value
                        ps.setHighestAcceptedValue(new OperationAndId(Operation.fromByteArray(msg.getOp()),
                                msg.getOpId()));
                    }
                    // Increment counter of prepareOks
                    ps.incrementPrepareOkCounter();
                }
                else {
                    //logger.debug("MSG == -1");
                    //logger.debug(ps.getHighestPrepareOk());

                    // If we have not received anything but bottoms
                    if (ps.getHighestPrepareOk() == -1) {
                        //logger.debug("Bottom increment");

                        // Increment counter of prepareOks
                        ps.incrementPrepareOkCounter();
                    }
                }

                // If majority quorum was achieved send accept messages to all
                if (ps.getPrepareOkCounter() >= ps.getQuorumSize()) {
                    //logger.debug("Quorum achieved");

                    // If highest prepare is -1, then our seqNumber was the winner
                    if (ps.getHighestPrepareOk() == -1)
                        ps.setHighestAcceptedValue(ps.getInitialProposal());
                    // TODO: mudei para accepted get initial proposal

                    cancelTimer(ps.getPrepareOkTimer());
                    // TODO: mudei para enviar accepted
                    OperationAndId opnId = ps.getHighestAcceptedValue();
                    for (Host h : ps.getMembership()) {
                        //logger.debug("Send Accept");
                        sendMessage(new AcceptMessage(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray(), ps.getSn()), h);
                    }

                    // Initializing timer that expires when a quorum of acceptOks is not achieved
                    ps.setAcceptOkTimer(setupTimer(new AcceptOkTimer(instance), 5000));
                }

            } else {
                //TODO: We have not yet received a JoinedNotification,
                // but we are already receiving messages from the other
                // agreement instances, maybe we should do something with them...?
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        try {
            int msgSn = msg.getSn();
            int instance = msg.getInstance();
            PaxosState ps = getPaxosInstance(instance);

            // If seqNumber of accept is equal or higher than our highest prepare
            if (msgSn >= ps.getHighestPrepare()) {
                ps.setHighestAccept(msgSn);
                OperationAndId opnId = new OperationAndId(Operation.fromByteArray(msg.getOp()),
                        msg.getOpId());
                ps.setHighestAcceptedValue(opnId);

                // Send acceptOk with that seqNumber and value to all learners
                for (Host h : ps.getMembership()) {
                    sendMessage(new AcceptOkMessage(instance, opnId.getOpId(),
                            opnId.getOperation().toByteArray(), msgSn), h);
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
            if (currentInstance >= 0 && instance >= currentInstance) {
                logger.debug("Entrei");


                // Update value and reset counter if the seqNumber is higher
                int highestAccept = ps.getHighestAccept();
                if (msg.getHighestAccept() > highestAccept) {
                    logger.debug("Bigger accept");
                    ps.resetAcceptOkCounter();
                    ps.incrementAcceptOkCounter();
                    ps.setHighestAccept(highestAccept);
                    ps.setHighestAcceptedValue(new OperationAndId(Operation.fromByteArray(msg.getOp()),
                            msg.getOpId()));
                }
                else if (msg.getHighestAccept() == highestAccept) {
                    logger.debug("Equal accept");
                    ps.incrementAcceptOkCounter();
                }

                //If majority quorum was achieved
                if (ps.getAcceptOkCounter() >= ps.getQuorumSize()) {
                    cancelTimer(ps.getAcceptOkTimer());
                    OperationAndId opnId = ps.getHighestAcceptedValue();
                    //If the quorum is for the current instance then decide
                    if (currentInstance == instance) {
                        triggerNotification(new DecidedNotification(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray()));
                        logger.debug(myself + " decided " + opnId.getOpId() + " on instance " + currentInstance);
                        currentInstance++;
                        // TODO: limpar tudo desta instancia pq já acabou??
                        // Execute all pending decisions, if there are any
                        ps = getPaxosInstance(currentInstance);
                        while (ps.getToDecide() != null) {
                            opnId = ps.getToDecide();
                            triggerNotification(new DecidedNotification(currentInstance++,
                                    opnId.getOpId(), opnId.getOperation().toByteArray()));
                            // TODO: limpar tudo destas instancias pq já acabaram??
                            ps = getPaxosInstance(currentInstance);
                        }
                    }
                    //If the quorum is not for the current instance save decision for later
                    else
                        ps.setToDecide(opnId);
                }

            } else {
                //TODO We have not yet received a JoinedNotification, but we are
                // already receiving messages from the other agreement instances,
                // maybe we should do something with them...?
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }


    /*--------------------------------- Notifications ------------------------------------ */

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        currentInstance = notification.getJoinInstance();
        PaxosState ps = getPaxosInstance(currentInstance);
        for(Host h : notification.getMembership()) {
            ps.addReplicaToMembership(h);
            openConnection(h);
        }
        logger.info("Agreement starting at instance {},  membership: {}", currentInstance, ps.getMembership());
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        try {
            logger.debug("Received " + request);
            int instance = request.getInstance();
            PaxosState ps = getPaxosInstance(instance);
            // Generating seqNumber
            ps.generateSn(myself);
            List<Host> membership = ps.getMembership();
            membership.forEach(h -> sendMessage(new PrepareMessage(ps.getSn(), instance), h));
            logger.debug("Sending to: " + membership);

            // Save proposed value
            ps.setInitialProposal(new OperationAndId(Operation.fromByteArray(request.getOperation()),
                    request.getOpId()));
            // Initializing timer that expires when a quorum of prepareOks is not achieved
            ps.setPrepareOkTimer(setupTimer(new PrepareOkTimer(instance), 5000));

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

    private void uponPrepareOkTimer(PrepareOkTimer prepareOkTimer, long timerId) {
        logger.debug("PrepareOkTimer Timeout");
        retryPrepareMessage(prepareOkTimer.getInstance());
    }

    private void uponAcceptOkTimer(AcceptOkTimer acceptOkTimer, long timerId) {
        logger.debug("AcceptOkTimer Timeout");
        retryPrepareMessage(acceptOkTimer.getInstance());
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private PaxosState getPaxosInstance(int instance) {
        if(!paxosByInstance.containsKey(instance))
            paxosByInstance.put(instance, new PaxosState());
        return paxosByInstance.get(instance);
    }

    private void retryPrepareMessage(int instance) {
        PaxosState ps = getPaxosInstance(instance);
        // Increasing seqNumber
        ps.increaseSn();
        List<Host> membership = ps.getMembership();
        membership.forEach(h -> sendMessage(new PrepareMessage(ps.getSn(), instance), h));
        logger.debug("Sending to: " + membership);
    }

}