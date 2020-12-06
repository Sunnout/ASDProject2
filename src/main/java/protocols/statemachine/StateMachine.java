package protocols.statemachine;

import io.netty.buffer.ByteBuf;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.statemachine.messages.CurrentStateMessage;
import protocols.statemachine.messages.AddReplicaMessage;
import protocols.statemachine.requests.RemoveReplicaRequest;
import protocols.statemachine.utils.OperationAndId;
import protocols.statemachine.utils.PaxosState;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.IncorrectAgreement;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;

import java.io.IOException;
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
    public final static String REMOVE_REPLICA = "remove";
    public final static byte MEMBERSHIP_OP_TYPE = 2;


    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private State state;

    // Estado atual
    private List<Host> membership;
    private Map<Integer, List<Host>> toSendState;
    private int nextInstance;
    private Map<Integer, PaxosState> previousPaxos;

    private List<OperationAndId> pendingOps;

    // ter uma mapeamento de operações para instancias de paxos (em que criamos uma classe que guarda
    // o estado do paxos - memberhsip, highest prepare, etc..)

    //ter um if que vê se vamos usar o paxos ou o multi paxos (se replica é lider fixe, senao
    // temos de redirecionar a operação para o lider e quando muda o lider temos de avisar a
    // state machine)

    // add replica e remove replica (quando se junta replica, dizer a posição da State Machine em que foi
    // adicionada para depois o paxos saber

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        nextInstance = 0;
        previousPaxos = new HashMap<>();
        pendingOps = new LinkedList<>();
        toSendState = new HashMap<>();

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
        registerMessageSerializer(channelId, CurrentStateMessage.MSG_ID, CurrentStateMessage.serializer);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(CurrentStateReply.REQUEST_ID, this::uponCurrentStateReply);

        /*--------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, AddReplicaMessage.MSG_ID, this::uponAddReplicaMsg, this::uponMsgFail);
        registerMessageHandler(channelId, CurrentStateMessage.MSG_ID, this::uponCurrentStateMsg, this::uponMsgFail);

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

        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(membership, 0));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            Random r = new Random();
            Host toContact = membership.get(r.nextInt(initialMembership.size()));
            openConnection(toContact);
            //You have to do something to join the system and know which instance you joined
            // (and copy the state of that instance)
        }

    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto){
        try {
            logger.debug("Received request: " + request);

            if ((state == State.ACTIVE) && pendingOps.size() == 0) {
                sendRequest(new ProposeRequest(nextInstance, request.getOpId(), request.getOperation()),
                        IncorrectAgreement.PROTOCOL_ID);
            }
            //Also do something starter, we don't want an infinite number of instances active
            //Maybe you should modify what is it that you are proposing so that you remember that this
            //operation was issued by the application (and not an internal operation, check the uponDecidedNotification)
            pendingOps.add(new OperationAndId(Operation.fromByteArray(request.getOperation()), request.getOpId()));
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    /*--------------------------------- Replies ---------------------------------------- */
    private void uponCurrentStateReply(CurrentStateReply reply, short sourceProto) {
        int instance = reply.getInstance();
        Iterator<Host> it = toSendState.get(instance).iterator();

        while (it.hasNext()) {
            Host h = it.next();
            sendMessage(new CurrentStateMessage(instance, reply.getState(), membership), h);
            it.remove();
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        try {
            logger.debug("Received notification: " + notification);
            Operation op = Operation.fromByteArray(notification.getOperation());

            if (notification.getInstance() == nextInstance) {
                if (pendingOps.get(0).equals(op))
                    pendingOps.remove(0);

                if (op.getOpType() != MEMBERSHIP_OP_TYPE)
                    triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));

                else {
                    String[] hostElements = op.getData().toString().split(":");
                    Host h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));

                    if (op.getKey().equals(ADD_REPLICA)) {
                        toSendState.get(nextInstance).add(h);

                        if (!membership.contains(h)) {
                            membership.add(h);
                            openConnection(h);

                            if (toSendState.containsValue(h))
                                sendRequest(new CurrentStateRequest(nextInstance), sourceProto);

                            //Avisar Paxos
                            //TODO como garantir que não começa instância de paxos antes de haver estado instalado?
                            sendRequest(new AddReplicaRequest(nextInstance, h), sourceProto);
                        }

                    } else {
                        if (membership.contains(h)) {
                            membership.remove(h);
                            closeConnection(h);
                            sendRequest(new RemoveReplicaRequest(nextInstance, h), sourceProto);
                        }
                    }
                }

                nextInstance++;

                PaxosState state;
                while (previousPaxos.containsKey(nextInstance)) {
                    state = previousPaxos.get(nextInstance++);
                    triggerNotification(new ExecuteNotification(state.getOpId(), state.getDecision().toByteArray()));
                }
            }

            previousPaxos.put(notification.getInstance(),
                    new PaxosState(op, notification.getOpId()));

            if (pendingOps.size() > 0) {
                OperationAndId opnId = pendingOps.get(0);
                sendRequest(new ProposeRequest(nextInstance, opnId.getOpId(), opnId.getOperation().toByteArray()),
                        IncorrectAgreement.PROTOCOL_ID);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponAddReplicaMsg(AddReplicaMessage msg, Host host, short sourceProto, int channelId) {
        // TODO: open connection?? bidirecionais??

        // TODO: passamos host em bytes
        Operation op = new Operation(MEMBERSHIP_OP_TYPE, ADD_REPLICA, host.toString().getBytes());
        pendingOps.add(new OperationAndId(op, UUID.randomUUID()));
    }

    private void uponCurrentStateMsg(CurrentStateMessage msg, Host host, short sourceProto, int channelId) {
        sendRequest(new InstallStateRequest(msg.getState()), sourceProto);
        nextInstance = msg.getInstance();
        state = State.ACTIVE;
        //TODO: enviar mensagem para ver se se passou mais alguma coisa entretanto?
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
        if (state == State.JOINING)
            sendMessage(new AddReplicaMessage(self), event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        //TODO remove replica after X tries
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
        //Maybe we don't want to do this forever. At some point we assume he is no longer there.
        //Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
        if (membership.contains(event.getNode()))
            openConnection(event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        //TODO: Precisar abrir conexão ao contrário?
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

}
