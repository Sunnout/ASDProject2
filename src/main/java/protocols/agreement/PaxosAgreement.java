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
import protocols.statemachine.notifications.ExecuteNotification;
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
    public final static int PREPARE_INDEX = 0;
    public final static int ACCEPT_INDEX = 1;

    private Host myself;
    private int joinedInstance;
    private List<Host> membership;
    //Map of highest prepare sequence number by instance
    private Map<Integer, Integer> highestPrepare;
    //Map of OperationAndId by instance, to store the highest prepared value
    private Map<Integer, OperationAndId> highestPreparedValue;
    //Map of highest accept sequence number by instance
    private Map<Integer, Integer> highestAccept;
    // Map of number of prepareOk by instance
    private Map<Integer, Integer> prepareOkCounters;
    //Map of highest accepted value by instance
    private Map<Integer, OperationAndId> highestAcceptedValue;
    //Map of decide values by instance
    private Map<Integer, OperationAndId> toDecide;
    //Map of numbers of accept ok by instance
    private Map<Integer, Integer> acceptOKCounters;
    //Our sequence number
    private int sn;
    //Current instance running
    private int currentInstance;
    //Map that stores an array of timerIds by instance , [0] = prepareOkTimer , [1] = acceptOkTimer
    private Map<Integer, Long[]> timersByInstance;

    public PaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        highestPrepare = new HashMap<>();
        highestPreparedValue = new HashMap<>();
        highestAcceptedValue = new HashMap<>();
        highestAccept = new HashMap<>();
        acceptOKCounters = new HashMap<>();
        prepareOkCounters = new HashMap<>();
        currentInstance = 0;

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
        registerMessageSerializer(cId, PrepareOkMessage.MSG_ID, PrepareOkMessage.serializer);
        registerMessageSerializer(cId, AcceptOkMessage.MSG_ID, AcceptOkMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareOkMessage.MSG_ID, this::uponPrepareOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage, this::uponMsgFail);

        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }


    /*--------------------------------- Messages ---------------------------------------- */


    private void uponAcceptOkMessage(AcceptOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            if (joinedInstance >= 0) {
                int instance = msg.getInstance();
                // initialize the highest Accept
                if (!highestAccept.containsKey(instance))
                    highestAccept.put(instance, -1);

                // Update value and reset counter if the seqnumb is higher
                if (msg.getHighestAccept() > highestAccept.get(instance)) {
                    acceptOKCounters.put(instance, 1);
                    highestAccept.put(instance, msg.getHighestAccept());
                    highestAcceptedValue.put(instance, new OperationAndId(Operation.fromByteArray(msg.getOp()), msg.getOpId()));
                }
                //Increment counter if seqnumb is the same
                else if(msg.getHighestAccept() == highestAccept.get(instance)) {
                    acceptOKCounters.put(instance, acceptOKCounters.get(instance) + 1);
                }

                //If majority quorum was achieved
                if (acceptOKCounters.get(instance) > (membership.size() / 2 + 1)) {
                    cancelTimer(timersByInstance.get(instance)[ACCEPT_INDEX]);
                    OperationAndId opnId = highestAcceptedValue.get(instance);
                    //If the quorum is for the current instance then decide
                    if(currentInstance == instance) {
                        currentInstance++;
                        triggerNotification(new DecidedNotification(instance, opnId.getOpId(), opnId.getOperation().toByteArray()));
                        // Execute all pending decisions, if there are any
                        while (toDecide.containsKey(currentInstance)) {
                            opnId = toDecide.get(currentInstance);
                            triggerNotification(new DecidedNotification(currentInstance++, opnId.getOpId(), opnId.getOperation().toByteArray()));
                        }
                    }
                    //Else save decision for later
                    else{
                        toDecide.put(instance,opnId);
                    }
                }

            }
            else {
                //TODO
                //We have not yet received a JoinedNotification, but we are already receiving messages from the other
                //agreement instances, maybe we should do something with them...?
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponPrepareOkMessage(PrepareOkMessage msg, Host host, short sourceProto, int channelId) {
        try {
            if (joinedInstance >= 0) {
                int instance = msg.getInstance();
                // initialize the highest Prepare
                if (!highestPrepare.containsKey(instance))
                    highestPrepare.put(instance, -1);

                // Update value and reset counter if the seqnumb is higher
                if (msg.getHighestPrepare() > highestPrepare.get(instance)) {
                    prepareOkCounters.put(instance, 1);
                    highestPrepare.put(instance, msg.getHighestPrepare());
                    highestPreparedValue.put(instance, new OperationAndId(Operation.fromByteArray(msg.getOp()), msg.getOpId()));
                }
                //Increment counter if seqnumb is the same
                else if(msg.getHighestPrepare() == highestPrepare.get(instance)) {
                    prepareOkCounters.put(instance, prepareOkCounters.get(instance) + 1);
                }

                //If majority quorum was achieved send Accept messages
                if (prepareOkCounters.get(instance) > (membership.size() / 2 + 1)) {
                    cancelTimer(timersByInstance.get(instance)[PREPARE_INDEX]);
                    OperationAndId opnId = highestPreparedValue.get(instance);
                    for (Host h : membership)
                        sendMessage(new AcceptMessage(instance, opnId.getOpId(), opnId.getOperation().toByteArray(), sn), h);

                    //intializing timer that expires when a quorum of acceptOk is not achieved
                    timersByInstance.get(instance)[ACCEPT_INDEX] = setupTimer(new AcceptOkTimer(instance), 5000);
                }

            } else {
                //TODO
                //We have not yet received a JoinedNotification, but we are already receiving messages from the other
                //agreement instances, maybe we should do something with them...?
            }
        } catch (IOException e) {
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
        joinedInstance = notification.getJoinInstance();
        currentInstance = joinedInstance;
        membership = new LinkedList<>(notification.getMembership());
        //TODO ver se current instance and joined instance sao os dois necessarios
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
            //Saved proposed Value and corresponding seqnumb
            highestPrepare.put(instance, sn);
            highestPreparedValue.put(instance, new OperationAndId(Operation.fromByteArray(request.getOperation()), request.getOpId()));
            //intializing timer that expires when a quorum of prepareOk is not achieved
            if (!timersByInstance.containsKey(instance))
                timersByInstance.put(instance, new Long[2]);

            timersByInstance.get(instance)[PREPARE_INDEX] = setupTimer(new PrepareOkTimer(instance), 5000);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private void uponAddReplicaRequest(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.add(request.getReplica());
    }

    private void uponRemoveReplicaRequest(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponPrepareOkTimer(PrepareOkTimer prepareOkTimer, long timerId) {
        logger.debug("Timeout of timer PrepareOkTimer");
        // increase sn by N
        sn = sn + membership.size();
        //TODO Fix currentInstance
        membership.forEach(h -> sendMessage(new PrepareMessage(sn,prepareOkTimer.getInstance()), h));
        logger.debug("Sending to: " + membership);



    }

    private void uponAcceptOkTimer(AcceptOkTimer acceptOkTimer, long timerId) {
        logger.debug("Timeout of timer acceptOkTimer");
        sn = sn + membership.size();
        membership.forEach(h -> sendMessage(new PrepareMessage(sn,acceptOkTimer.getInstance()), h));
        logger.debug("Sending to: " + membership);

    }
    /*--------------------------------- Auxiliar Methods ---------------------------------------- */

    private void sendProposes(int sn, int instance){

    }
}
