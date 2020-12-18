package protocols.agreement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptOkMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.messages.PrepareOkMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.NewLeaderNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.requests.SameReplicasRequest;
import protocols.agreement.timers.MultiPaxosStartTimer;
import protocols.agreement.timers.PaxosTimer;
import protocols.agreement.utils.PaxosState;
import protocols.app.utils.Operation;
import protocols.statemachine.messages.ProposeToLeaderMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.utils.OperationAndId;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class MultiPaxosAgreement extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(MultiPaxosAgreement.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 500;
    public final static String PROTOCOL_NAME = "MultiPaxosAgreement";


    private Host myself;
    private Host currentLeader;
    private int currentInstance; // Instance that is currently running
    private int currentSn;
    private Map<Integer, PaxosState> paxosByInstance; // PaxosState for each instance

    public MultiPaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        currentInstance = -1; // -1 means we have not yet joined the system
        paxosByInstance = new HashMap<>();
        currentLeader = null;

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(PaxosTimer.TIMER_ID, this::uponPaxosTimer);
        registerTimerHandler(MultiPaxosStartTimer.TIMER_ID, this::uponMultiPaxosStartTimer);

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

            // If the message is not from an instance that has already ended
            if (instance >= currentInstance) {
                if(currentLeader == null) {
                    logger.debug("uponPrepareMessage: new leader is {}", host);
                    currentLeader = host;
                    triggerNotification(new NewLeaderNotification(currentLeader));
                }

                if(currentLeader.compareTo(host) == 0) {
                    logger.debug("uponPrepareMessage: sending prepare ok to {}", host);
                    currentSn = msg.getSn();
                    sendMessage(new PrepareOkMessage(instance, null, null, -1, currentSn), host);
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

            // If the message is not from an instance that has already ended
            // and we don't have a majority of prepareOks
            if (instance >= currentInstance && !ps.havePrepareOkMajority()) {
                int msgSn = msg.getSn();
                logger.debug("uponPrepareOkMessage: MsgSn: {}, MsgInstance: {}", msgSn, instance);
                ps.incrementPrepareOkCounter();

                // If majority quorum was achieved
                if (ps.getPrepareOkCounter() >= ps.getQuorumSize()) {
                    logger.debug("uponPrepareOkMessage: Got PrepareOk majority");
                    ps.setPrepareOkMajority(true);

                    OperationAndId opnId = ps.getInitialProposal();

                    // Send accept messages to all
                    for (Host h : ps.getMembership()) {
                        sendMessage(new AcceptMessage(instance, opnId.getOpId(),
                                opnId.getOperation().toByteArray(), currentSn), h);
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

            // If the message is not from an instance that has already ended
            if (instance >= currentInstance && (currentLeader != null && host.compareTo(currentLeader) == 0)) {
                logger.debug("uponAcceptMessage: got accept from leader");
                int msgSn = msg.getSn();
                PaxosState ps = getPaxosInstance(instance);
                logger.debug("uponAcceptMessage: MsgSn: {}, MsgInstance: {}", msgSn, instance);

                OperationAndId opnId = new OperationAndId(Operation.fromByteArray(msg.getOp()), msg.getOpId());
                // Send acceptOk with that seqNumber and value to all learners

                logger.debug("uponAcceptMessage: Membership: {}", ps.getMembership());
                for (Host h : ps.getMembership()) {
                    sendMessage(new AcceptOkMessage(instance, opnId.getOpId(),
                            opnId.getOperation().toByteArray(), msgSn), h);
                }
                logger.debug("uponAcceptMessage: Sent AcceptOkMessages");
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

                logger.debug("uponAcceptOkMessage: MsgSn: {}, MsgInstance: {}", msg.getHighestAccept(), instance);
                ps.addHostToHaveAccepted(host);

                // If majority quorum was achieved
                if (ps.hasAcceptOkQuorum() && ps.getToDecide() == null) {
                    logger.debug("uponAcceptOkMessage: Got AcceptOk majority");

                    // Cancel PaxosTimer for this instance
                    logger.debug("uponAcceptOkMessage: cancelled PaxosTimer {}", ps.getPaxosTimer());
                    cancelTimer(ps.getPaxosTimer());

                    OperationAndId opnId = new OperationAndId(Operation.fromByteArray(msg.getOp()), msg.getOpId());
                    ps.setToDecide(opnId);

                    // If the quorum is for the current instance then decide
                    if (currentInstance == instance) {
                        logger.debug("uponAcceptOkMessage: Decided {} in instance {}", opnId.getOpId(), instance);
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
            PaxosState ps = getPaxosInstance(currentInstance);

            // Initialize membership and open connections
            for (Host h : notification.getMembership()) {
                ps.addReplicaToMembership(h);
                openConnection(h);
            }

            ps.setMembershipOk();

            logger.info("Agreement starting at instance {},  membership: {}",
                    currentInstance, ps.getMembership());

            // If we decided a value before joining, trigger the decide
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

        logger.debug("uponProposeRequest: New Propose");

        if (currentLeader == null) {
            Random r = new Random();
            setupTimer(new MultiPaxosStartTimer(request), r.nextInt(5000));
        }

        else {
            proposeRequest(request);
        }
    }

    private void proposeRequest(ProposeRequest request){
        //TODO verificar ps.membershipIsOk()
        int instance = request.getInstance();
        PaxosState ps = getPaxosInstance(instance);

        try{
            // Get membership from last instance and use it for this instance
            // because we already know if we added, removed or stayed the same
            // Send prepares to every node in membership
            List<Host> membership = ps.getMembership();

            // If I dont know who the leader is, send prepares
            if(currentLeader == null){
                ps.generateSn(myself);
                currentSn = ps.getSn();
                membership.forEach(h -> sendMessage(new PrepareMessage(currentSn, instance), h));
                logger.debug("uponProposeRequest: Sent Prepares");

                // If I am the leader, send accepts, always with the same sn
            } else if(currentLeader.compareTo(myself) == 0){
                logger.debug("uponProposeRequest: Sent Accepts");
                membership.forEach(h -> sendMessage(new AcceptMessage(instance, request.getOpId(),
                        request.getOperation(), currentSn), h));
            }

            // Save proposed value
            ps.setInitialProposal(new OperationAndId(Operation.fromByteArray(request.getOperation()),
                    request.getOpId()));

            // Setup PaxosTimer that expires if we don't decide
            long timerId = setupTimer(new PaxosTimer(instance), 2000);
            ps.setPaxosTimer(timerId);
            logger.debug("uponProposeRequest: New PaxosTimer created with id {}", timerId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uponAddReplicaRequest(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        Host replica = request.getReplica();
        PaxosState ps = getPaxosInstance(request.getInstance());
        // Adding replica to membership of instance and opening connection
        ps.addReplicaToMembership(replica);
        openConnection(replica);
        // Membership up to date
        ps.setMembershipOk();
    }

    private void uponRemoveReplicaRequest(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        Host replica = request.getReplica();
        PaxosState ps = getPaxosInstance(request.getInstance());
        // Removing replica from membership of instance and closing connection
        ps.removeReplicaFromMembership(replica);
        closeConnection(replica);
        // Membership up to date
        ps.setMembershipOk();
    }

    private void uponSameReplicasRequest(SameReplicasRequest request, short sourceProto) {
        logger.debug("Received " + request);
        PaxosState ps = getPaxosInstance(request.getInstance());
        // Membership up to date
        ps.setMembershipOk();
    }

    /*--------------------------------- Timers ---------------------------------------- */

    private void uponPaxosTimer(PaxosTimer paxosTimer, long timerId) {
        int instance = paxosTimer.getInstance();
        PaxosState ps = getPaxosInstance(instance);

        // If we have not decided yet, retry prepare
        if (ps.getToDecide() == null) {
            logger.debug("PaxosTimer Timeout with id: {}", timerId);
            // Increasing seqNumber
            ps.increaseSn();
            ps.resetPrepareOkCounter();
            ps.setPrepareOkMajority(false);
            ps.resetHaveAccepted();
            List<Host> membership = ps.getMembership();
            membership.forEach(h -> sendMessage(new PrepareMessage(ps.getSn(), instance), h));
            logger.debug("uponPaxosTimer: Retry sending to: " + membership);

            // Setup new PaxosTimer that expires if we don't decide
            long newTimerId = setupTimer(new PaxosTimer(instance), 2000);
            ps.setPaxosTimer(newTimerId);
            logger.debug("uponPaxosTimer: New PaxosTimer created with id {}", newTimerId);
        }
    }

    private void uponMultiPaxosStartTimer(MultiPaxosStartTimer paxosTimer, long timerId) {
        if (currentLeader == null){
            proposeRequest(paxosTimer.getRequest());
        }
    }

    /*--------------------------------- Procedures ---------------------------------------- */

    private PaxosState getPaxosInstance(int instance) {
        // If we haven't initialized the instance
        if (!paxosByInstance.containsKey(instance)) {
            paxosByInstance.put(instance, new PaxosState());

            PaxosState paxos = paxosByInstance.get(instance);

            if(instance > 0) {
                for (Host h : paxosByInstance.get(instance - 1).getMembership())
                    paxos.addReplicaToMembership(h);
            }
        }

        return paxosByInstance.get(instance);
    }
}