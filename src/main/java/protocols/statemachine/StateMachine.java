package protocols.statemachine;

import protocols.agreement.PaxosAgreement;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.SameReplicasRequest;
import protocols.app.HashApp;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.statemachine.messages.AddReplicaReply;
import protocols.statemachine.messages.AddReplicaMessage;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.timers.HostDeadTimer;
import protocols.statemachine.utils.OperationAndId;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * This is NOT fully functional StateMachine implementation.
 * This is simply an example of things you can do, and can be used as a starting point.
 * <p>
 * You are free to change/delete anything in this class, including its fields.
 * The only thing that you cannot change are the notifications/requests between the StateMachine and the APPLICATION
 * You can change the requests/notification between the StateMachine and AGREEMENT protocol, however make sure it is
 * coherent with the specification shown in the project description.
 * <p>
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class StateMachine extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    public final static String ADD_REPLICA = "add";
    public final static String REM_REPLICA = "remove";
    public final static byte MEMBERSHIP_OP_TYPE = 2;

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private State state;

    // Estado atual
    private List<Host> membership; // Current membership
    private int currentInstance; // Current State Machine instance
    private List<OperationAndId> pendingOps; // List of pending operations
    private Map<Integer, OperationAndId> decidedOps; // Decided operation by instance
    private Map<Integer, Host> replicasToSendState; // Host to send state in an instance
    private Map<Integer, Integer> hostFailures; // Map that stores numbers of failures of each host

    //TODO: ter um if que vê se vamos usar o paxos ou o multi paxos (se replica é lider fixe, senao
    // temos de redirecionar a operação para o lider e quando muda o lider temos de avisar a
    // state machine)


    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        currentInstance = 0;
        pendingOps = new LinkedList<>();
        decidedOps = new HashMap<>();
        replicasToSendState = new HashMap<>();
        hostFailures = new HashMap<>();

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Message Serializers ----------------------------- */
        registerMessageSerializer(channelId, AddReplicaMessage.MSG_ID, AddReplicaMessage.serializer);
        registerMessageSerializer(channelId, AddReplicaReply.MSG_ID, AddReplicaReply.serializer);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(HostDeadTimer.TIMER_ID, this::uponHostDeadTimer);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(CurrentStateReply.REQUEST_ID, this::uponCurrentStateReply);

        /*--------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, AddReplicaMessage.MSG_ID, this::uponAddReplicaMsg, this::uponMsgFail);
        registerMessageHandler(channelId, AddReplicaReply.MSG_ID, this::uponAddReplicaReply, this::uponMsgFail);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);
    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));

        String host = props.getProperty("initial_membership");
        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();
        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }
        logger.info("My initial membership {} ", initialMembership);


        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            logger.info("My initial membership {} ", initialMembership);
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(membership, 0));

        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            Random r = new Random();
            // Create connection to random node in membership
            Host toContact = initialMembership.get(r.nextInt(initialMembership.size()));
            openConnection(toContact);
            membership = new LinkedList<>();
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */

    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        try {
            logger.debug("Received request: " + request);

            // If I am active and have not proposed anything in this instance, propose the value
            if (state == State.ACTIVE && pendingOps.size() == 0) {
                logger.debug("Proposed {} in instance {}", request.getOpId(), currentInstance);
                sendRequest(new ProposeRequest(currentInstance, request.getOpId(), request.getOperation()),
                        PaxosAgreement.PROTOCOL_ID);
            }
            // Add value to list of pending operations until it is decided
            pendingOps.add(new OperationAndId(Operation.fromByteArray(request.getOperation()), request.getOpId()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*--------------------------------- Replies ---------------------------------------- */

    private void uponCurrentStateReply(CurrentStateReply reply, short sourceProto) {
        int instance = reply.getInstance();
        Host h = replicasToSendState.remove(instance);
        logger.debug("Current state reply, send to {} in instance {}", h, instance);

        if (h != null) {
            logger.debug("Sending membership: {} to {} in instance {}", membership, h, instance);
            sendMessage(new AddReplicaReply(instance, reply.getState(), membership), h);
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        try {
            logger.debug("Decided {} in instance {}", notification.getOpId(), notification.getInstance());
            Operation op = Operation.fromByteArray(notification.getOperation());
            int instance = notification.getInstance();

            boolean isMyOp = false;

            if (pendingOps.size() > 0) {
                isMyOp = pendingOps.get(0).getOpId().compareTo(notification.getOpId()) == 0;

                // If we proposed this operation, remove it from the pending list
                if (isMyOp)
                    pendingOps.remove(0);
            }

            logger.debug("Decision was mine in instance {}? {}", currentInstance, isMyOp);

            // The decided operation was from the application, so we notify it
            if (op.getOpType() != MEMBERSHIP_OP_TYPE) {
                triggerNotification(new ExecuteNotification(notification.getOpId(),
                        notification.getOperation()));
                sendRequest(new SameReplicasRequest(instance + 1), PaxosAgreement.PROTOCOL_ID);
            }

            // The decided operation was from the membership
            else
                processMembershipChange(op, isMyOp, instance, sourceProto);

            // Move on to next instance of State Machine
            currentInstance++;

            // If there are pending operations, propose the first one
            if (pendingOps.size() > 0) {
                OperationAndId opnId = pendingOps.get(0);
                logger.debug("Proposed {} in instance {}", opnId.getOpId(), currentInstance);

                sendRequest(new ProposeRequest(currentInstance, opnId.getOpId(), opnId.getOperation().toByteArray()),
                        PaxosAgreement.PROTOCOL_ID);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponAddReplicaMsg(AddReplicaMessage msg, Host host, short sourceProto, int channelId) {
        try {
            logger.debug("Received Add replica msg from {}", host);
            Operation op = new Operation(MEMBERSHIP_OP_TYPE, ADD_REPLICA, hostToByteArray(host));
            pendingOps.add(new OperationAndId(op, UUID.randomUUID()));
            if (pendingOps.size() == 1) {
                OperationAndId opnId = pendingOps.get(0);
                sendRequest(new ProposeRequest(currentInstance, opnId.getOpId(), opnId.getOperation().toByteArray()),
                        PaxosAgreement.PROTOCOL_ID);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponAddReplicaReply(AddReplicaReply msg, Host host, short sourceProto, int channelId) {
        logger.info("Trying to Add replica {}", host);
        // Request state installation from App
        sendRequest(new InstallStateRequest(msg.getState()), HashApp.PROTO_ID);
        // Initialize this node
        currentInstance = msg.getInstance();
        membership = msg.getMembership();
        // Open connection to membership
        for (Host h : membership)
            openConnection(h);
        state = State.ACTIVE;
        // Assuming that state was successfully installed
        logger.debug("Joined notification in instance {}", currentInstance);
        triggerNotification(new JoinedNotification(membership, currentInstance));
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponHostDeadTimer(HostDeadTimer hostDeadTimer, long timerId) {
        tryConnection(hostDeadTimer.getHost());
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
        // Request to join the system
        if (state == State.JOINING)
            sendMessage(new AddReplicaMessage(self), event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
        processRemoveHost(event.getNode());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host h = event.getNode();
        int port = h.getPort();
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());

        // Increment failure counter for this host
        if (hostFailures.get(port) == null)
            hostFailures.put(port, 1);
        else
            hostFailures.put(port, hostFailures.get(port) + 1);

        // If connection to host has failed many time, remove host
        if (hostFailures.get(port) == 30) {
            processRemoveHost(h);

        } else {
            // Setup new HostDeadTimer to wait a little before retrying
            setupTimer(new HostDeadTimer(h), 500);
            logger.debug("New HostDeadTimer created for host {}", port);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Procedures ---------------------------- */

    private void processMembershipChange(Operation op, boolean isMyOp, int instance, short sourceProto) throws IOException {
        Host h = hostFromByteArray(op.getData());
        logger.debug("Processing Membership Operation with host {}", h);


        // Operation to add host to membership
        if (op.getKey().equals(ADD_REPLICA)) {
            logger.debug("Membership Operation to add {} in instance {}", h, instance);
            // Add this host to the list to send state and request state from App
            if (isMyOp) {
                logger.debug("Sending request for state of instance {} ", instance);
                replicasToSendState.put(instance + 1, h);
                sendRequest(new CurrentStateRequest(instance + 1), HashApp.PROTO_ID);
            }

            membership.add(h);
            openConnection(h);

            // Warn Paxos that a replica joined the system in an instance
            sendRequest(new AddReplicaRequest(instance + 1, h), PaxosAgreement.PROTOCOL_ID);
        }

        // Operation to remove host from membership
        else {
            logger.debug("Membership Operation to remove {} in instance {}", h, instance);
            membership.remove(h);
            closeConnection(h);
            sendRequest(new RemoveReplicaRequest(instance + 1, h), PaxosAgreement.PROTOCOL_ID);
        }
    }

    private void tryConnection(Host h) {
        if (membership.contains(h))
            openConnection(h);
    }

    private void processRemoveHost(Host h) {
        try {
            logger.debug("Connection to host {} died, going to remove him", h);

            // Add remove operation to head of list, to minimize no outgoing connection errors
            Operation op = new Operation(MEMBERSHIP_OP_TYPE, REM_REPLICA, hostToByteArray(h));
            if (pendingOps.size() == 0)
                pendingOps.add(0, new OperationAndId(op, UUID.randomUUID()));
            else
                pendingOps.add(1, new OperationAndId(op, UUID.randomUUID()));

            if (pendingOps.size() == 1) {
                OperationAndId opnId = pendingOps.get(0);
                sendRequest(new ProposeRequest(currentInstance, opnId.getOpId(), opnId.getOperation().toByteArray()),
                        PaxosAgreement.PROTOCOL_ID);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private byte[] hostToByteArray(Host h) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] address = h.getAddress().getAddress();
        dos.writeInt(address.length);
        dos.write(address);
        dos.writeShort(h.getPort());
        return baos.toByteArray();
    }

    private Host hostFromByteArray(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        byte[] address = new byte[dis.readInt()];
        dis.read(address, 0, address.length);
        short port = dis.readShort();
        return new Host(InetAddress.getByAddress(address), port);
    }

}
