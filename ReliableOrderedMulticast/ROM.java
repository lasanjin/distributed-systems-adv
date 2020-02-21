import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import mcgui.*;

/**
 * Reliable and Ordered Multicaster (ROM)
 * 
 * @author Sanjin & Svante
 */
public class ROM extends Multicaster {

    /**
     * Holds ring topology <id, cpu> of network based on ids (ids are mapped to
     * addresses in the mcgui package)
     */
    private Map<Integer, Integer> nodes;
    /**
     * Keeps track of requests <id> during election in case neighbor fails and we
     * need to contact successor
     */
    private Set<Integer> requests;
    /**
     * All received messages, <sequence number, message>.
     */
    private Set<ROMMessage> deliveredMessages;
    /**
     * Keeps not delivered messages in queue so we can resend in case node sends
     * messsages during leader election or sequencer fails.
     */
    private Set<ROMMessage> queuedMessages;
    /**
     * Keeps track of the next message expected from sender, <id, message number>.
     */
    private Map<Integer, Integer> nextMessage;
    /**
     * Pending messages that have been received before messages previous to them
     * according to messageNum.
     */
    private List<ROMMessage> pendingMessages;
    /**
     * Id of sequencer.
     */
    private Integer sequencer;
    /**
     * Random parameter for leader election. Highest cpu capacity gets elected.
     */
    private Integer cpu;
    /**
     * Sequence number for total order of messages. Managed by elected sequencer.
     */
    private Integer seqNum;
    /**
     * Local sequence number for total order of messages.
     */
    private Integer locSeqNum;
    /**
     * Message number to keep order of sent messages.
     */
    private Integer messageNum;
    /**
     * Ongoing leader election.
     */
    private boolean isElection;

    /**
     * Initialize node.
     */
    public void init() {
        this.nodes = buildRingTop();
        this.requests = new HashSet<>();
        this.deliveredMessages = new HashSet<>();
        this.queuedMessages = new HashSet<>();
        this.nextMessage = new HashMap<>();
        this.pendingMessages = new ArrayList<>();
        this.sequencer = null;
        this.cpu = (new Random()).nextInt(999);
        this.seqNum = 0;
        this.locSeqNum = 0;
        this.messageNum = 0;
        this.isElection = false;

        // mcui.debug("Number of hosts: " + hosts);
        mcui.debug("Node ID: " + id);
        mcui.debug("CPU: " + cpu);
    }

    /**
     * Build ring topology based on hosts from Multicaster because we don't have
     * access to anything else.
     */
    private Map<Integer, Integer> buildRingTop() {
        this.nodes = new HashMap<>();

        for (int i = 0; i < hosts; i++) {
            this.nodes.put(i, Integer.MIN_VALUE);
        }

        return nodes;
    }

    /**
     * Initiates leader election based on cpu capacity by broadcasting INIT_ELECTION
     * message.
     */
    private void initElection() {
        if (!this.isElection) {
            ROMMessage msg = new ROMMessage(id, MessageType.INIT_ELECTION);
            broadcast(msg);

            execElection();
        }
    }

    /**
     * Broadcast custom message to all nodes except self
     * 
     * @param msg The message to broadcast.
     */
    private void broadcast(ROMMessage msg) {
        for (Integer nodeId : this.nodes.keySet()) {
            /* Send to everyone except self */
            if (nodeId != id) {
                bcom.basicsend(nodeId, msg);
            }
        }
    }

    /**
     * Ads self to candidates for leader election and sends candidates to neighbor.
     */
    private void execElection() {
        if (!this.isElection) {
            /* Reset variables before taking part in election */
            this.isElection = true;
            this.sequencer = null;
            this.requests.clear();
            /* If first time election, pick node with highest cpu capacity */
            this.nodes.replace(id, cpu);
            if (this.locSeqNum == 0) {
            }
            /*
             * If sequencer has crashes then elect node with highest local sequence number
             */
            else {
                this.nodes.replace(id, this.locSeqNum);
            }
            /* Send candidates to neighbor */
            forwardCandidates(this.nodes);
        }
    }

    /**
     * Finds neighbor, builds election message and sends candidates to neighbor.
     * 
     * @param candidates List of candidates for leader election.
     */
    private void forwardCandidates(Map<Integer, Integer> candidates) {
        Integer neighbor = findNeighbor();
        /* Build election message */
        ROMMessage msg = new ROMMessage(id, MessageType.ELECTION);
        msg.setCandidates(candidates);
        /*
         * Keep track of request so we can resend candidates to successor if node fails
         * during leader election.
         */
        this.requests.add(neighbor);
        bcom.basicsend(neighbor, msg);
    }

    /**
     * Finds neighbor based on ring topology (min to max id).
     */
    private Integer findNeighbor() {
        // return (id + 1) % hosts;

        List<Integer> nodes = new ArrayList<>(this.nodes.keySet());
        /* Sort nodes by id */
        Collections.sort(nodes);
        int index = nodes.indexOf(id);
        /* Return lowest if this.id is max */
        if (index >= nodes.size() - 1) {
            return nodes.get(0);
        }

        return nodes.get(index + 1);
    }

    /**
     * Update nodes in case nodes fail during leader election and we must resend
     * updated list of candidates (this.nodes)
     * 
     * @param candidates The received candidates from neighbor for the leader
     *                   election.
     */
    private void updateNodes(Map<Integer, Integer> candidates) {
        for (Integer nodeId : this.nodes.keySet()) {
            /* Remove nodes that have crashed during leader election */
            if (!candidates.containsKey(nodeId)) {
                this.nodes.remove(nodeId);
            }
            /*
             * Received candidates don't contain this.cpu value (unless candidates have
             * propagated full cirlce) -> don't overwrite this.cpu value
             */
            else if (nodeId != id) {
                this.nodes.replace(nodeId, candidates.get(nodeId));
            }
        }
    }

    /**
     * Leader election process. Propagates candidates if to neighbor if received
     * candidates contain cpu > this.cpu.
     * 
     * @param candidates List of candidates during leader election.
     */
    private void electLeader(Map<Integer, Integer> candidates) {
        /*
         * Compare cpu for 1st leader election and local sequence number if sequencer
         * fails and new leader election is initiated.
         */
        Integer value = this.locSeqNum == 0 ? this.cpu : this.locSeqNum;
        /* Candidates have propagated full circle */
        if (candidates.containsKey(id) && candidates.get(id).equals(value)) {
            this.isElection = false;
            /* Find id with max cpu */
            this.sequencer = selectLeader(candidates);

            mcui.debug("Sequencer selected: " + this.sequencer);

            /* Set sequencenumber for sequencer */
            setSeqNum();
            /* Reset requests */
            this.requests.clear();
            /* Build and broadcast coordination message */
            ROMMessage msg = new ROMMessage(id, MessageType.COORDINATION);
            msg.setSequencer(this.sequencer);
            broadcast(msg);
            /* Send queued messages to new sequencer */
            sendQueuedMessages();
        }
        /* Only send to neighbor if received candidates contain cpu > this.cpu */
        else if (Collections.max(candidates.values()).compareTo(value) > 0) {
            /* Add self to candidates */
            candidates.replace(id, cpu);
            forwardCandidates(candidates);
        }
    }

    /**
     * Finds id of candidate with maximum cpu capacity or local sequence number.
     * 
     * @param candidates List of all candidates from a leader election.
     * @return Id of max cpu capacity or local sequence number.
     */
    private Integer selectLeader(Map<Integer, Integer> candidates) {
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
     */
    private void setSeqNum() {
        /* Set most recent sequence number for elected sequencer */
        if (Integer.valueOf(id).equals(this.sequencer)) {
            this.seqNum = this.locSeqNum + 1;
        }
    }

    /**
     * Sends queued messages to sequencer.
     */
    private void sendQueuedMessages() {
        Iterator<ROMMessage> q = this.queuedMessages.iterator();

        while (q.hasNext()) {
            bcom.basicsend(this.sequencer, q.next());
        }
    }

    /**
     * Delivers message based on sender.
     * 
     * @param msg Message to be delivered.
     */
    private void receiveMessage(ROMMessage msg) {
        boolean delivered = false;
        /* I am sequencer */
        if (Integer.valueOf(id).equals(this.sequencer)) {
            delivered = deliverMessageSequencer(msg);
        }
        /* Message received with sequence number */
        else if (msg.getSeqNum() != null) {
            delivered = deliverMessageNode(msg);
        }
        /*
         * Message received with no sequence number. Happens when elected sequencer
         * receives a message before coordination message.
         */
        else {
            this.pendingMessages.add(msg);
        }
        /*
         * If message m wasn't delivered then check if we are have pending messages to
         * deliver before m
         */
        if (!delivered) {
            deliverPendingMessages(msg.getInitialSender());
        }
    }

    /**
     * Delivers message by FIFO order at sequencer.
     * 
     * @param msg The message to deliver.
     */
    private boolean deliverMessageSequencer(ROMMessage msg) {
        boolean delivered = true;
        /* If node has not delivered message already */
        if (!this.deliveredMessages.contains(msg)) {
            Integer sender = msg.getInitialSender();
            Integer messageNum = this.nextMessage.getOrDefault(sender, 0) + 1;
            /* If the message is the next one expected from sender */
            if (messageNum.equals(msg.getMessageNum())) {
                /* Deliver message */
                this.deliveredMessages.add(msg);
                mcui.deliver(msg.getInitialSender(), msg.getText());
                /* Remove delivered message from queue */
                removeQueuedMessages(msg);
                /* Update next expected message from sender */
                this.nextMessage.put(sender, messageNum);
                /* Set sequence number and broadcast */
                msg.setSeqNum(this.seqNum);
                broadcast(msg);
                /* Increment sequence number and deliver message */
                this.seqNum++;
                /* Send any queued messages to elected sequencer */
                sendQueuedMessages();
            }
            /* A message has been received from sender before its predecessor */
            else {
                /* Wait until we receive its predecessor */
                this.pendingMessages.add(msg);
                delivered = false;
            }
        }

        return delivered;
    }

    /**
     * Delivers message by FIFO order at nodes.
     * 
     * @param msg The message to deliver.
     */
    private boolean deliverMessageNode(ROMMessage msg) {
        boolean delivered = true;
        /* If node has not delivered message already */
        if (!this.deliveredMessages.contains(msg)) {
            int compare = msg.getSeqNum().compareTo(this.locSeqNum + 1);
            /* If sequence number from sequencer is expected */
            if (compare == 0) {
                /* Update local sequence number */
                this.locSeqNum++;
                /* Deliver message */
                this.deliveredMessages.add(msg);
                mcui.deliver(msg.getInitialSender(), msg.getText());
                /* Remove delivered message from queue */
                removeQueuedMessages(msg);
                /* Update next expected message from sender */
                Integer sender = msg.getInitialSender();
                this.nextMessage.put(sender, this.nextMessage.getOrDefault(sender, 0) + 1);
                /*
                 * Broadcast delivered message from sequencer to other nodes in case sequencer
                 * crashed and only this node received message.
                 */
                multicastDeliveredMessage(msg);
                /* Send any queued messages to elected sequencer */
                sendQueuedMessages();
            }
            /* A message has been received from sequencer before its predecessor */
            else if (compare > 0) {
                /* Wait until we receive its predecessor */
                this.pendingMessages.add(msg);
                delivered = false;
            }
        }

        return delivered;
    }

    /**
     * Broadcasts message to all nodes except self and sequencer.
     * 
     * @param msg The message to broadcast.
     */
    private void multicastDeliveredMessage(ROMMessage msg) {
        for (Integer nodeId : nodes.keySet()) {

            if (nodeId != id && nodeId != this.sequencer) {
                bcom.basicsend(nodeId, msg);
            }
        }
    }

    /**
     * Looks for any pending messages from sender that have not been delivered yet.
     * 
     * @param sender The intial sender of the message.
     */
    private void deliverPendingMessages(int sender) {
        for (ROMMessage msg : this.pendingMessages) {
            /*
             * If pending message is from sender and it is the next message expected from
             * sender
             */
            if (msg.getInitialSender() == sender
                    && msg.getMessageNum().equals(this.nextMessage.getOrDefault(sender, 0) + 1)) {
                receiveMessage(msg);
            }
        }
    }

    /**
     * Removes queued message from initial sender of message.
     * 
     * @param msg The message from sender to remove from queue.
     */
    private void removeQueuedMessages(ROMMessage msg) {
        /* Remove message if it was sent it */
        if (id == msg.getInitialSender()) {
            this.queuedMessages.remove(msg);
        }
    }

    /**
     * Sets elected sequencer and empties any queued messages and requests.
     * 
     * @param msg The message with elected sequencer.
     */
    private void coordinate(ROMMessage msg) {
        this.isElection = false;
        this.sequencer = msg.getSequencer();

        mcui.debug("Sequencer elected: " + this.sequencer);

        setSeqNum();
        /* Send any queued messages to elected sequencer */
        sendQueuedMessages();
        /* Reset requests */
        this.requests.clear();
    }

    /**
     * * * * * * * * * * * * * * * * Multicaster * * * * * * * * * * * * * * * * /
     */

    /**
     * The GUI calls this module to multicast a message
     * 
     * @param messagetext The message casted.
     */
    public void cast(String messagetext) {
        /* Increment local message number and queue message */
        ROMMessage msg = new ROMMessage(id, messagetext, ++this.messageNum);
        this.queuedMessages.add(msg);

        if (this.sequencer == null) {
            initElection();
        }
        /* Send message to sequencer */
        else {
            /* Send any queued messages to elected sequencer */
            sendQueuedMessages();
        }
    }

    /**
     * Receive a basic message.
     * 
     * @param peer    Sender of message.
     * @param message The message received.
     */
    public void basicreceive(int peer, Message message) {
        ROMMessage msg = (ROMMessage) message;

        switch (msg.getType()) {
            /* Message received */
            case MESSAGE:
                receiveMessage(msg);
                break;
            /* Initiate new leader election */
            case INIT_ELECTION:
                execElection();
                break;
            /* Ongoing election */
            case ELECTION:
                Map<Integer, Integer> candidates = msg.getCandidates();

                updateNodes(candidates);
                electLeader(candidates);
                break;
            /* Sequencer elected */
            case COORDINATION:
                coordinate(msg);
                break;
        }

    }

    /**
     * Signals that a peer is down and has been down for a while to allow for
     * messages taking different paths from this peer to arrive.
     * 
     * @param peer The dead node.
     */
    public void basicpeerdown(int peer) {
        mcui.debug("Peer down: " + peer);

        /* Remove failed node from nodes */
        this.nodes.remove(peer);
        /* Sequencer failed, initiate new leader election */
        if (Integer.valueOf(peer) == this.sequencer) {
            initElection();
        }
        /*
         * Failed to reach neighbor during leader election -> resend updated nodes as
         * candidates
         */
        else if (this.requests.contains(peer)) {
            /* Remove request to failed node */
            this.requests.remove(peer);
            /* Continue with leader election and send candidates to neighbor successor */
            execElection();
        }
    }
}