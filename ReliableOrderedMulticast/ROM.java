import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.Set;

import mcgui.*;

/**
 * Reliable and Ordered Multicaster (ROM)
 * 
 * @author Sanjin & Svante
 */
public class ROM extends Multicaster {

    /**
     * Ring leader election algorithm.
     */
    protected RingLeaderElection RLE;
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
     * Pending messages that have been received from sender before messages
     * preceding them according to messageNum.
     */
    private Map<Integer, List<ROMMessage>> pendingMessages;
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
    private Boolean isElection;

    /**
     * Initialize node.
     */
    public void init() {
        this.RLE = new RingLeaderElection(bcom, mcui);
        this.nodes = ROMUtils.buildRingTop(hosts);
        this.requests = new HashSet<>();
        this.deliveredMessages = new HashSet<>();
        this.queuedMessages = new HashSet<>();
        this.nextMessage = new HashMap<>();
        this.pendingMessages = new HashMap<>();
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
     * Delivers message based on sender.
     * 
     * @param msg Message to be delivered.
     */
    public void receiveMessage(ROMMessage msg) {
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
            ROMUtils.addPendingMessage(this, msg.getInitialSender(), msg);
        }
        /*
         * If message m wasn't delivered then check if we are have pending messages to
         * deliver before m
         */
        if (!delivered) {
            ROMUtils.deliverPendingMessages(this, msg.getInitialSender());
        }
    }

    /**
     * Delivers message by Causal Order at sequencer.
     * 
     * @param msg The message to deliver.
     */
    private boolean deliverMessageSequencer(ROMMessage msg) {
        boolean delivered = true;
        /* If node has not delivered message already (Integrity) */
        if (!this.deliveredMessages.contains(msg)) {
            Integer sender = msg.getInitialSender();
            Integer messageNum = this.nextMessage.getOrDefault(sender, 0) + 1;
            /* If the message is the next one expected from sender */
            if (messageNum.equals(msg.getMessageNum())) {
                /* Deliver message (Validity) */
                this.deliveredMessages.add(msg);
                mcui.deliver(sender, msg.getText());
                /* Remove delivered message from queue */
                ROMUtils.removeQueuedMessages(this, msg);
                /* Update next expected message from sender */
                this.nextMessage.put(sender, messageNum);
                /* Set sequence number and broadcast */
                msg.setSeqNum(this.seqNum);
                ROMUtils.broadcast(bcom, this, msg);
                /* Increment sequence number and deliver message */
                this.seqNum++;
                /* Send any queued messages to elected sequencer */
                ROMUtils.sendQueuedMessages(bcom, this);
            }
            /* A message has been received from sender before its predecessor */
            else {
                /* Wait until we receive its predecessor */
                ROMUtils.addPendingMessage(this, sender, msg);
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
        /* If node has not delivered message already (Integrity) */
        if (!this.deliveredMessages.contains(msg)) {
            Integer sender = msg.getInitialSender();
            int compare = msg.getSeqNum().compareTo(this.locSeqNum + 1);
            /* If sequence number from sequencer is expected */
            if (compare == 0) {
                /* Update local sequence number */
                this.locSeqNum++;
                /* Deliver message (Validity) */
                this.deliveredMessages.add(msg);
                mcui.deliver(sender, msg.getText());
                /* Remove delivered message from queue */
                ROMUtils.removeQueuedMessages(this, msg);
                /* Update next expected message from sender */
                this.nextMessage.put(sender, this.nextMessage.getOrDefault(sender, 0) + 1);
                /*
                 * Broadcast delivered message from sequencer to other nodes in case sequencer
                 * crashed and only this node received message. (Agreement)
                 */
                ROMUtils.multicastDeliveredMessage(bcom, this, msg);
                /* Send any queued messages to elected sequencer */
                ROMUtils.sendQueuedMessages(bcom, this);
            }
            /* A message has been received from sequencer before its predecessor */
            else if (compare > 0) {
                /* Wait until we receive its predecessor */
                ROMUtils.addPendingMessage(this, sender, msg);
                delivered = false;
            }
        }

        return delivered;
    }

    /**
     * * * * * * * * * * * * * * * * MULTICASTER * * * * * * * * * * * * * * * * /
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
            RLE.initElection(this);
        }
        /* Send message to sequencer */
        else {
            /* Send any queued messages to elected sequencer */
            ROMUtils.sendQueuedMessages(bcom, this);
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
                RLE.execElection(this);
                break;
            /* Ongoing election */
            case ELECTION:
                Map<Integer, Integer> candidates = msg.getCandidates();

                RLE.updateNodes(this, candidates);
                RLE.electLeader(this, candidates);
                break;
            /* Sequencer elected */
            case COORDINATION:
                RLE.coordinate(this, msg);
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
        if (Integer.valueOf(peer).equals(this.sequencer)) {
            RLE.initElection(this);
        }
        /*
         * Failed to reach neighbor during leader election -> resend updated nodes as
         * candidates
         */
        else if (this.requests.contains(peer)) {
            /* Remove request to failed node */
            this.requests.remove(peer);
            /* Continue with leader election and send candidates to neighbor successor */
            RLE.execElection(this);
        }
    }

    /**
     * * * * * * * * * * * * * * * GETTERS & SETTERS * * * * * * * * * * * * * * * /
     */

    /**
     * Add request for sending candidates to neighbor.
     * 
     * @param neighbor The neighbor to send candidates to for leader election.
     */
    public void addRequest(Integer neighbor) {
        this.requests.add(neighbor);
    }

    /**
     * Resets the requests done sending candidates to neighbor during leader
     * election.
     */
    public void clearRequests() {
        this.requests.clear();
    }

    /**
     * @param suqNum The sequence number to set.
     */
    public void setSeqNum(Integer seqNum) {
        this.seqNum = seqNum;
    }

    /**
     * @param sequencer the sequencer to set
     */
    public void setSequencer(Integer sequencer) {
        this.sequencer = sequencer;
    }

    /**
     * Resets sequencer (Sets sequencer = null)
     */
    public void resetSequencer() {
        this.sequencer = null;
    }

    /**
     * @param isElection the isElection to set
     */
    public void setIsElection(Boolean isElection) {
        this.isElection = isElection;
    }

    public int getId() {
        return super.getId();
    }

    /**
     * @return the nodes
     */
    public Map<Integer, Integer> getNodes() {
        return this.nodes;
    }

    /**
     * Updates node value (cpu capacity or local sequence number).
     * 
     * @param id    The nodes id.
     * @param value The value mapped to the node (cpu capacity or local sequence
     *              number).
     * @return The replaced value.
     */
    public Integer replaceNode(int id, Integer value) {
        return this.nodes.replace(id, value);
    }

    /**
     * @return the deliveredMessages
     */
    public Set<ROMMessage> getDeliveredMessages() {
        return this.deliveredMessages;
    }

    /**
     * @return the queuedMessages
     */
    public Set<ROMMessage> getQueuedMessages() {
        return this.queuedMessages;
    }

    /**
     * Removes message from queued messages (not delivered).
     * 
     * @param msg The message to remove.
     */
    public void removeQueuedMessage(ROMMessage msg) {
        this.queuedMessages.remove(msg);
    }

    /**
     * @return the nextMessage
     */
    public Map<Integer, Integer> getNextMessage() {
        return this.nextMessage;
    }

    /**
     * @return the pendingMessages
     */
    public Map<Integer, List<ROMMessage>> getPendingMessages() {
        return this.pendingMessages;
    }

    /**
     * Puts a message mapped to the intital sender of the message in pending
     * messages.
     * 
     * @param sender The initial sender of the message.
     * @param msgs   The list of message from the sender.
     */
    public void putPendingMessage(Integer sender, List<ROMMessage> msgs) {
        this.pendingMessages.putIfAbsent(sender, msgs);
    }

    /**
     * @return the sequencer
     */
    public Integer getSequencer() {
        return this.sequencer;
    }

    /**
     * @return the cpu
     */
    public Integer getCpu() {
        return this.cpu;
    }

    /**
     * @return the seqNum
     */
    public Integer getSeqNum() {
        return this.seqNum;
    }

    /**
     * @return the locSeqNum
     */
    public Integer getLocSeqNum() {
        return this.locSeqNum;
    }

    /**
     * @return the isElection
     */
    public Boolean getIsElection() {
        return this.isElection;
    }

}
