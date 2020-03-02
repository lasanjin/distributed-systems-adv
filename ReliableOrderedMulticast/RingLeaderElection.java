import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Ring leader election algorithm for electing centralized sequencer.
 * 
 * @author Sanjin & Svante
 */
public class RingLeaderElection {

    /**
     * The communication backend.
     */
    private mcgui.BasicCommunicator bcom;

    /**
     * The user interface.
     */
    private mcgui.MulticasterUI mcui;

    public RingLeaderElection(mcgui.BasicCommunicator bcom, mcgui.MulticasterUI mcui) {
        this.bcom = bcom;
        this.mcui = mcui;
    }

    /**
     * Initiates leader election based on cpu capacity by broadcasting INIT_ELECTION
     * message.
     * 
     * @param rom Reliable Ordered Multicaster.
     */
    public void initElection(ROM rom) {
        if (!rom.getIsElection()) {

            int id = rom.getId();
            ROMMessage msg = new ROMMessage(id, MessageType.INIT_ELECTION);
            ROMUtils.broadcast(bcom, rom, msg);

            resetNodes(rom);
            execElection(rom);
        }
    }

    /**
     * Reset known nodes values for new leader election.
     * 
     * @param rom Reliable Ordered Multicaster.
     */
    private void resetNodes(ROM rom) {
        rom.getNodes().replaceAll((k, v) -> Integer.MIN_VALUE);
    }

    /**
     * Ads self to candidates for leader election and sends candidates to neighbor.
     *
     * @param rom Reliable Ordered Multicaster.
     */
    public void execElection(ROM rom) {
        /* Reset variables before taking part in election */
        rom.setIsElection(true);
        rom.resetSequencer();
        rom.clearRequests();
        /* If first time election, pick node with highest cpu capacity */
        int id = rom.getId();
        Integer locSeqNum = rom.getLocSeqNum();

        if (locSeqNum == 0) {
            rom.updateNodeValue(id, rom.getCpu());
        }
        /*
         * If sequencer has crashes then elect node with highest local sequence number
         */
        else {
            rom.updateNodeValue(id, locSeqNum);
        }
        /* Send candidates to neighbor */
        forwardCandidates(rom, rom.getNodes());
    }

    /**
     * Finds neighbor, builds election message and sends candidates to neighbor.
     * 
     * @param rom        Reliable Ordered Multicaster.
     * @param candidates List of candidates for leader election.
     */
    public void forwardCandidates(ROM rom, Map<Integer, Integer> candidates) {
        Integer neighbor = findNeighbor(rom);
        /* Build election message */
        ROMMessage msg = new ROMMessage(rom.getId(), MessageType.ELECTION);
        msg.setCandidates(candidates);
        /*
         * Keep track of request so we can resend candidates to successor if node fails
         * during leader election.
         */
        rom.addRequest(neighbor);
        this.bcom.basicsend(neighbor, msg);
    }

    /**
     * Finds neighbor based on ring topology (min to max id).
     *
     * @param rom Reliable Ordered Multicaster.
     */
    public Integer findNeighbor(ROM rom) {
        // return (id + 1) % hosts;
        List<Integer> nodeIds = new ArrayList<>(rom.getNodes().keySet());
        /* Sort nodes by id */
        Collections.sort(nodeIds);
        int index = nodeIds.indexOf(rom.getId());
        /* Return lowest if this.id is max */
        if (index >= nodeIds.size() - 1) {
            return nodeIds.get(0);
        }

        return nodeIds.get(index + 1);
    }

    /**
     * Update nodes in case nodes fail during leader election and we must resend
     * updated list of candidates (this.nodes)
     * 
     * @param rom        Reliable Ordered Multicaster.
     * @param candidates List of candidates for leader election.
     */
    public void updateNodes(ROM rom, Map<Integer, Integer> candidates) {
        for (Integer nodeId : rom.getNodes().keySet()) {
            /* Remove nodes that have crashed during leader election */
            if (!candidates.containsKey(nodeId)) {
                rom.removeNode(nodeId);
            }
            /*
             * Received candidates don't contain this.cpu value (unless candidates have
             * propagated full cirlce) -> don't overwrite this.cpu value
             */
            else if (nodeId != rom.getId()) {
                rom.updateNodeValue(nodeId, candidates.get(nodeId));
            }
        }
    }

    /**
     * Leader election process. Propagates candidates if to neighbor if received
     * candidates contain value > this.value.
     * 
     * @param rom        Reliable Ordered Multicaster.
     * @param candidates List of candidates for leader election.
     */
    public void electLeader(ROM rom, Map<Integer, Integer> candidates) {
        /*
         * Compare cpu capacity value for 1st leader election and local sequence number
         * if sequencer fails and new leader election is initiated.
         */
        Integer locSeqNum = rom.getLocSeqNum();
        Integer value = locSeqNum == 0 ? rom.getCpu() : locSeqNum;
        /* Candidates have propagated full circle */
        int id = rom.getId();
        if (candidates.containsKey(id) && candidates.get(id).equals(value)) {
            rom.setIsElection(false);
            /* Find id with max value */
            rom.setSequencer(selectLeader(candidates));
            Integer sequencer = rom.getSequencer();

            mcui.debug("Sequencer selected: " + sequencer);

            /* Set sequencenumber for sequencer */
            setSeqNum(rom);
            /* Reset requests */
            rom.clearRequests();
            /* Build and broadcast coordination message */
            ROMMessage msg = new ROMMessage(id, MessageType.COORDINATION);
            msg.setSequencer(sequencer);
            ROMUtils.broadcast(bcom, rom, msg);
            /* Send queued messages to new sequencer */
            ROMUtils.sendQueuedMessages(bcom, rom);
        }
        /* Only send to neighbor if received candidates contain value > this.value */
        else if (Collections.max(candidates.values()).compareTo(value) > 0) {
            /* Add self to candidates */
            candidates.replace(id, value);
            forwardCandidates(rom, candidates);
        }
    }

    /**
     * Finds id of candidate with maximum cpu capacity or local sequence number.
     * 
     * @param candidates List of all candidates from a leader election.
     * @return Id of max cpu capacity or local sequence number.
     */
    public Integer selectLeader(Map<Integer, Integer> candidates) {
        Entry<Integer, Integer> max = null;

        for (Entry<Integer, Integer> c : candidates.entrySet()) {
            if (max == null || c.getValue().compareTo(max.getValue()) > 0) {
                max = c;
            }
        }

        return max.getKey();
    }

    /**
     * Sets sequence number for elected sequencer.
     * 
     * @param rom Reliable Ordered Multicaster.
     */
    private void setSeqNum(ROM rom) {
        /* Set most recent sequence number for elected sequencer */
        if (Integer.valueOf(rom.getId()).equals(rom.getSequencer())) {
            rom.setSeqNum(rom.getLocSeqNum() + 1);
        }
    }

    /**
     * Sets elected sequencer and empties any queued messages and requests.
     *
     * @param rom Reliable Ordered Multicaster.
     * @param msg The message with elected sequencer.
     */
    public void coordinate(ROM rom, ROMMessage msg) {
        rom.setIsElection(false);
        rom.setSequencer(msg.getSequencer());

        mcui.debug("Sequencer elected: " + rom.getSequencer());

        setSeqNum(rom);
        /* Reset requests */
        rom.clearRequests();
        /* Send any queued messages to elected sequencer */
        ROMUtils.sendQueuedMessages(bcom, rom);
    }
}
