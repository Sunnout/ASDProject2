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

    //Index of timers in timersByInstance map:
    // [0] = prepareOkTimer
    public final static int PREPARE_INDEX = 0;
    // [1] = acceptOkTimer
    public final static int ACCEPT_INDEX = 1;

    private Host myself;
    private List<Host> membership;
    private int joinedInstance;
    private int currentInstance; // Instance that is currently running
    private int sn; //Our sequence number

    private Map<Integer, Integer> highestPrepare; //Map of highest prepare sequence number by instance
    private Map<Integer, OperationAndId> highestPreparedValue; //Map of highest prepared value by instance
    private Map<Integer, Integer> prepareOkCounters; // Map of number of prepareOk by instance

    private Map<Integer, Integer> highestAccept; //Map of highest accept sequence number by instance
    private Map<Integer, OperationAndId> highestAcceptedValue; //Map of highest accepted value by instance
    private Map<Integer, Integer> acceptOkCounters; //Map of numbers of accept ok by instance

    private Map<Integer, List<Host>> replicasToAdd; //Map of hosts to add in an instance
    private Map<Integer, List<Host>> replicasToRemove; //Map of hosts to remove in an instance

    private Map<Integer, OperationAndId> toDecide; //Map of decide values by instance
    private Map<Integer, Long[]> timersByInstance; //Map that stores an array of timerIds by instance


    public PaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        currentInstance = 0;

        highestPrepare = new HashMap<>();
        highestPreparedValue = new HashMap<>();
        prepareOkCounters = new HashMap<>();

        highestAccept = new HashMap<>();
        highestAcceptedValue = new HashMap<>();
        acceptOkCounters = new HashMap<>();

        replicasToAdd = new HashMap<>();
        replicasToRemove = new HashMap<>();

        toDecide = new HashMap<>();
        timersByInstance = new HashMap<>();

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
        try {
            int msgSn = msg.getSn();
            int msgInstance = msg.getInstance();

            // If seqNumber of prepare is higher than out highest prepare send prepareOk
            if (msgSn > highestPrepare.get(msgInstance)) {
                highestPrepare.put(msgInstance, msgSn);
                int snToSend = -1;

                if (highestAccept.containsKey(msgInstance))
                    snToSend = highestAccept.get(msgInstance);

                OperationAndId operationToSend = highestAcceptedValue.get(msgInstance);
                // If we have not accepted anything, send bottoms (nulls)
                UUID opId = null;
                byte[] op = null;

                // If we have accepted something, send highest accepted seqNumber and value
                if (operationToSend != null) {
                    opId = operationToSend.getOpId();
                    op = operationToSend.getOperation().toByteArray();
                }

                sendMessage(new PrepareOkMessage(msgInstance, opId, op, snToSend), host);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponPrepareOkMessage(PrepareOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            if (joinedInstance >= 0) {
                int instance = msg.getInstance();
                int msgPrepare = msg.getHighestPrepare();

                // If the prepareOk didn't return bottoms and the seqNumber is higher than the one
                // we have, we replace the value with the new one
                if (msgPrepare != -1 && msgPrepare > highestPrepare.get(instance)) {
                    highestPrepare.put(instance, msgPrepare);
                    highestPreparedValue.put(instance, new OperationAndId(Operation.fromByteArray(msg.getOp()),
                            msg.getOpId()));
                }

                // Initialize counter of prepareOks if it does not exists
                if (!prepareOkCounters.containsKey(instance))
                    prepareOkCounters.put(instance, 1);
                // Increment counter of prepareOks
                else
                    prepareOkCounters.put(instance, prepareOkCounters.get(instance) + 1);

                // If majority quorum was achieved send accept messages to all
                if (prepareOkCounters.get(instance) > (membership.size() / 2 + 1)) {
                    cancelTimer(timersByInstance.get(instance)[PREPARE_INDEX]);
                    OperationAndId opnId = highestPreparedValue.get(instance);
                    for (Host h : membership)
                        sendMessage(new AcceptMessage(instance, opnId.getOpId(), opnId.getOperation().toByteArray(), sn), h);

                    // Initializing timer that expires when a quorum of acceptOks is not achieved
                    timersByInstance.get(instance)[ACCEPT_INDEX] = setupTimer(new AcceptOkTimer(instance), 5000);
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
            int msgInstance = msg.getInstance();

            // If seqNumber of accept is equal or higher than our highest prepare
            if (msgSn >= highestPrepare.get(msgInstance)) {
                highestAccept.put(msgInstance, msgSn);
                OperationAndId opnId = new OperationAndId(Operation.fromByteArray(msg.getOp()), msg.getOpId());
                highestAcceptedValue.put(msgInstance, opnId);

                // Send acceptOk with that seqNumber and value to all learners
                for (Host h : membership) {
                    sendMessage(new AcceptOkMessage(msgInstance, opnId.getOpId(),
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

            // To prevent from deciding more that once in the same instance
            if (joinedInstance >= 0 && instance >= currentInstance) {
                // Initialize the highest accept
                if (!highestAccept.containsKey(instance))
                    highestAccept.put(instance, -1);

                // Update value and reset counter if the seqNumber is higher
                if (msg.getHighestAccept() > highestAccept.get(instance)) {
                    acceptOkCounters.put(instance, 1);
                    highestAccept.put(instance, msg.getHighestAccept());
                    highestAcceptedValue.put(instance, new OperationAndId(Operation.fromByteArray(msg.getOp()), msg.getOpId()));
                }
                // TODO: Increment counter if values are the same or the seqNumbers???
                else if (msg.getHighestAccept() == highestAccept.get(instance)) {
                    acceptOkCounters.put(instance, acceptOkCounters.get(instance) + 1);
                }

                //If majority quorum was achieved
                if (acceptOkCounters.get(instance) > (membership.size() / 2 + 1)) {
                    cancelTimer(timersByInstance.get(instance)[ACCEPT_INDEX]);
                    OperationAndId opnId = highestAcceptedValue.get(instance);
                    //If the quorum is for the current instance then decide
                    if (currentInstance == instance) {
                        currentInstance++;
                        triggerNotification(new DecidedNotification(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray()));
                        // TODO: limpar tudo desta instancia pq já acabou??
                        manageMembership();
                        // Execute all pending decisions, if there are any
                        while (toDecide.containsKey(currentInstance)) {
                            opnId = toDecide.remove(currentInstance);
                            triggerNotification(new DecidedNotification(currentInstance++,
                                    opnId.getOpId(), opnId.getOperation().toByteArray()));
                            // TODO: limpar tudo destas instancias pq já acabaram??
                            manageMembership();
                        }
                    }
                    //If the quorum is not for the current instance save decision for later
                    else
                        toDecide.put(instance, opnId);
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
        //TODO ver se current instance and joined instance sao os dois necessarios
        joinedInstance = notification.getJoinInstance();
        currentInstance = joinedInstance;
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        try {
            logger.debug("Received " + request);

            //TODO choose sequence Number
            sn = 0;
            int instance = request.getInstance();
            membership.forEach(h -> sendMessage(new PrepareMessage(sn, instance), h));
            logger.debug("Sending to: " + membership);
            //Saved proposed Value and corresponding seqNumber
            highestPrepare.put(instance, -1);
            highestPreparedValue.put(instance, new OperationAndId(Operation.fromByteArray(request.getOperation()), request.getOpId()));
            //intializing timer that expires when a quorum of prepareOk is not achieved
            if (!timersByInstance.containsKey(instance))
                timersByInstance.put(instance, new Long[2]);

            timersByInstance.get(instance)[PREPARE_INDEX] = setupTimer(new PrepareOkTimer(instance), 5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponAddReplicaRequest(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        int replicaInstance = request.getInstance();
        Host replica = request.getReplica();
        if(replicaInstance == currentInstance)
            membership.add(replica);
        else {
            // TODO: precisamos de listas de hosts ou só entra um em cada instance?
            if(!replicasToAdd.containsKey(replicaInstance))
                replicasToAdd.put(replicaInstance, new LinkedList<>());

            replicasToAdd.get(replicaInstance).add(replica);
        }
    }

    private void uponRemoveReplicaRequest(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        int replicaInstance = request.getInstance();
        Host replica = request.getReplica();
        if(replicaInstance == currentInstance)
            membership.remove(replica);
        else {
            // TODO: precisamos de listas de hosts ou só sai um em cada instance?
            if(!replicasToRemove.containsKey(replicaInstance))
                replicasToRemove.put(replicaInstance, new LinkedList<>());

            replicasToRemove.get(replicaInstance).add(replica);
        }
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponPrepareOkTimer(PrepareOkTimer prepareOkTimer, long timerId) {
        logger.debug("Timeout of timer PrepareOkTimer");
        sn += membership.size();
        membership.forEach(h -> sendMessage(new PrepareMessage(sn, prepareOkTimer.getInstance()), h));
        logger.debug("Sending to: " + membership);
    }

    private void uponAcceptOkTimer(AcceptOkTimer acceptOkTimer, long timerId) {
        logger.debug("Timeout of timer acceptOkTimer");
        sn += membership.size();
        membership.forEach(h -> sendMessage(new PrepareMessage(sn, acceptOkTimer.getInstance()), h));
        logger.debug("Sending to: " + membership);
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void manageMembership() {
        for(Host h : replicasToRemove.get(currentInstance))
            membership.remove(h);
        replicasToRemove.remove(currentInstance);

        for(Host h : replicasToAdd.get(currentInstance))
            membership.add(h);
        replicasToAdd.remove(currentInstance);
    }

}