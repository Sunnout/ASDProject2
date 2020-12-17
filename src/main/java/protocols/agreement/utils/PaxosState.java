package protocols.agreement.utils;

import protocols.statemachine.utils.OperationAndId;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class PaxosState {

    private int sn; // Current sequence number
    private List<Host> membership; // Membership for this instance
    private OperationAndId initialProposal; // Initial proposed value

    private int highestPrepare; // Highest prepared seqNumber
    private int highestPrepareOk; // Highest prepareOk seqNumber
    private int prepareOkCounter; /// Number of prepareOks for the same seqNumber

    private int highestAccept; // Highest accepted seqNumber
    private OperationAndId highestAcceptedValue; // Highest accepted value

    private int highestLearned; // Highest learned seqNumber
    private OperationAndId highestLearnedValue; // Highest learned value
    private Set<Host> haveAccepted; // set of hosts who have sent acceptOks

    private int maxSnAccept; // Biggest seqNumber of accepted value in prepareOk

    private int highestSeenSn; // Highest seen seqNumber

    private Host replicaToRemove; // Host to remove

    private OperationAndId toDecide; // Value to decide

    private long paxosTimer; // Id of prepareOkTimer

    private boolean prepareOkMajority;


    public PaxosState() {
        this.sn = -1;
        this.membership = new LinkedList<>();
        this.initialProposal = null;
        this.highestPrepare = -1;
        this.highestPrepareOk = -1;
        this.prepareOkCounter = 0;
        this.highestAccept = -1;
        this.highestAcceptedValue = null;
        this.haveAccepted = new HashSet<>();
        this.highestLearned = -1;
        this.highestLearnedValue = null;
        this.maxSnAccept = -1;
        this.highestSeenSn = -1;
        this.replicaToRemove = null;
        this.toDecide = null;
        this.paxosTimer = -1;
        this.prepareOkMajority = false;
    }

    public int getSn() {
        return this.sn;
    }

    public void generateSn(Host myself) {
        Collections.sort(membership, new HostComparator());
        this.sn = membership.indexOf(myself) + 1;
    }

    public void increaseSn() {
        Random r = new Random();
        do {
            int multiplier = r.nextInt(5) + 1;
            this.sn += multiplier * getMembershipSize();
        } while(this.sn < this.highestSeenSn);
    }

    public List<Host> getMembership() {
        return this.membership;
    }

    public int getMembershipSize() {
        return this.membership.size();
    }

    public int getQuorumSize() {
        return this.membership.size() / 2 + 1;
    }

    public void addReplicaToMembership(Host replica) {
        if(!replica.equals(this.replicaToRemove))
            this.membership.add(replica);
    }

    public void removeReplicaFromMembership(Host replica) {
        this.membership.remove(replica);
        this.replicaToRemove = replica;
    }

    public int getHighestPrepare() {
        return highestPrepare;
    }

    public void setHighestPrepare(int highestPrepare) {
        this.highestPrepare = highestPrepare;
        if(this.highestPrepare > this.highestSeenSn)
            this.highestSeenSn = this.highestPrepare;
    }

    public int getHighestPrepareOk() {
        return highestPrepareOk;
    }

    public void setHighestPrepareOk(int highestPrepareOk) {
        this.highestPrepareOk = highestPrepareOk;
    }

    public OperationAndId getInitialProposal() {
        return initialProposal;
    }

    public void setInitialProposal(OperationAndId initialProposal) {
        this.initialProposal = initialProposal;
    }

    public int getPrepareOkCounter() {
        return prepareOkCounter;
    }

    public void resetPrepareOkCounter() {
        this.prepareOkCounter = 0;
    }

    public void incrementPrepareOkCounter() {
        this.prepareOkCounter += 1;
    }

    public int getHighestAccept() {
        return highestAccept;
    }

    public void setHighestAccept(int highestAccept) {
        this.highestAccept = highestAccept;
        if(this.highestAccept > this.highestSeenSn)
            this.highestSeenSn = this.highestAccept;
    }

    public OperationAndId getHighestAcceptedValue() {
        return highestAcceptedValue;
    }

    public void setHighestAcceptedValue(OperationAndId highestAcceptedValue) {
        this.highestAcceptedValue = highestAcceptedValue;
    }

    public int getHighestLearned() {
        return highestLearned;
    }

    public void setHighestLearned(int highestLearned) {
        this.highestLearned = highestLearned;
        if(this.highestLearned > this.highestSeenSn)
            this.highestSeenSn = this.highestLearned;
    }

    public OperationAndId getHighestLearnedValue() {
        return highestLearnedValue;
    }

    public void setHighestLearnedValue(OperationAndId highestLearnedValue) {
        this.highestLearnedValue = highestLearnedValue;
    }

    public int getMaxSnAccept() {
        return maxSnAccept;
    }

    public void setMaxSnAccept(int maxSnAccept) {
        this.maxSnAccept = maxSnAccept;
        if(this.maxSnAccept > this.highestSeenSn)
            this.highestSeenSn = this.maxSnAccept;
    }

   public void addHostToHaveAccepted(Host h){
        haveAccepted.add(h);
   }
   public void resetHaveAccepted(){
        haveAccepted.clear();
   }
   public boolean hasAcceptOkQuorum(){
        if(membership.size() == 0)
            return false;
        return haveAccepted.size() == getQuorumSize();
   }
   

    public OperationAndId getToDecide() {
        return toDecide;
    }

    public void setToDecide(OperationAndId toDecide) {
        this.toDecide = toDecide;
    }

    public long getPaxosTimer() {
        return paxosTimer;
    }

    public void setPaxosTimer(long paxosTimer) {
        this.paxosTimer = paxosTimer;
    }
    public boolean havePrepareOkMajority() {
        return prepareOkMajority;
    }

    public void setPrepareOkMajority(boolean prepareOkMajority) {
        this.prepareOkMajority = prepareOkMajority;
    }

}