package protocols.agreement.utils;

import protocols.statemachine.utils.OperationAndId;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class MultiPaxosState {

    private int sn; // Current sequence number
    private List<Host> membership; // Membership for this instance
    private OperationAndId initialProposal; // Initial proposed value

    private int prepareOkCounter; /// Number of prepareOks for the same seqNumber

    private Set<Host> haveAccepted; // set of hosts who have sent acceptOks

    private OperationAndId toAcceptOpnId;
    private int toAcceptSn;

    private int highestSeenSn; // Highest seen seqNumber

    private Host replicaToRemove; // Host to remove

    private OperationAndId toDecide; // Value to decide

    private long paxosLeaderTimer; // Id of prepareOkTimer

    private boolean prepareOkMajority;

    private boolean isMembershipOk;


    public MultiPaxosState() {
        this.sn = -1;
        this.membership = new LinkedList<>();
        this.initialProposal = null;
        this.prepareOkCounter = 0;
        this.haveAccepted = new HashSet<>();
        this.toAcceptOpnId = null;
        this.toAcceptSn = -1;
        this.highestSeenSn = -1;
        this.replicaToRemove = null;
        this.toDecide = null;
        this.paxosLeaderTimer = -1;
        this.prepareOkMajority = false;
        this.isMembershipOk = false;
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

    public OperationAndId getToAcceptOpnId() {
        return toAcceptOpnId;
    }

    public void setToAcceptOpnId(OperationAndId toAcceptOpnId) {
        this.toAcceptOpnId = toAcceptOpnId;
    }

    public int getToAcceptSn() {
        return toAcceptSn;
    }

    public void setToAcceptSn(int toAcceptSn) {
        this.toAcceptSn = toAcceptSn;
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

    public long getPaxosLeaderTimer() {
        return paxosLeaderTimer;
    }

    public void setPaxosLeaderTimer(long paxosLeaderTimer) {
        this.paxosLeaderTimer = paxosLeaderTimer;
    }
    public boolean havePrepareOkMajority() {
        return prepareOkMajority;
    }

    public void setPrepareOkMajority(boolean prepareOkMajority) {
        this.prepareOkMajority = prepareOkMajority;
    }

    public boolean isMembershipOk() {
        return isMembershipOk;
    }

    public void setMembershipOk() {
        this.isMembershipOk = true;
    }

}