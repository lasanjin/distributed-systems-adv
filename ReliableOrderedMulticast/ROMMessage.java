import java.util.Map;

import mcgui.*;

/**
 * Message implementation for Reliable and Ordered Multicaster (ROM).
 *
 * @author Sanjin & Svante
 */
public class ROMMessage extends Message {

    /**
     * Generated in terminal: $ serialver ROMMessage.
     */
    private static final long serialVersionUID = 5725921430379858860L;
    /**
     * Unique Id.
     */
    private String uid;
    /**
     * Type of message.
     */
    private MessageType type;
    /**
     * Initial sender of message.
     */
    private Integer initialSender;
    /**
     * Text of message.
     */
    private String text;
    /**
     * Sequence number of message.
     */
    private Integer seqNum;
    /**
     * Message number from sender.
     */
    private Integer messageNum;

    /**
     * Elected sequencer.
     */
    private Integer sequencer;
    /**
     * List of candidates for leader election.
     */
    private Map<Integer, Integer> candidates;

    /**
     * @param sender The id of the sender.
     * @param text   The text of the message.
     */
    public ROMMessage(int sender, String text, Integer messageNum) {
        super(sender);
        this.uid = ROMMessageUtils.genUID();
        this.initialSender = sender;
        this.text = text;
        this.messageNum = messageNum;
        this.type = MessageType.MESSAGE;
    }

    /**
     * 
     * @param sender The id of the sender.
     * @param type   The type of message.
     */
    public ROMMessage(int sender, MessageType type) {
        super(sender);
        this.type = type;
    }

    public void setSender(Integer sender) {
        super.sender = sender;
    }

    /**
     * 
     * @param text The text of the message.
     */
    public void setText(String text) {
        this.text = text;
    }

    /**
     * 
     * @param seqNum The sequence number of message.
     */
    public void setSeqNum(Integer seqNum) {
        this.seqNum = seqNum;
    }

    /**
     * 
     * @param sequencer The elected sequencer after leader election.
     */
    public void setSequencer(Integer sequencer) {
        this.sequencer = sequencer;
    }

    /**
     * 
     * @param candidates The candidates for the leader election.
     */
    public void setCandidates(Map<Integer, Integer> candidates) {
        this.candidates = candidates;
    }

    /**
     * 
     * @return The sender of the message.
     */
    public int getSender() {
        return super.sender;
    }

    /**
     * 
     * @return The initial sender of the message.
     */
    public int getInitialSender() {
        return this.initialSender;
    }

    /**
     * 
     * @return The type of the message.
     */
    public MessageType getType() {
        return this.type;
    }

    /**
     * 
     * @return The text of the message.
     */
    public String getText() {
        return this.text;
    }

    /**
     * 
     * @return Sequence number of message from sequencer.
     */
    public Integer getSeqNum() {
        return this.seqNum;
    }

    /**
     * 
     * @return The number of the message from sender.
     */
    public Integer getMessageNum() {
        return this.messageNum;
    }

    /**
     * 
     * @return The elected sequencer.
     */
    public Integer getSequencer() {
        return this.sequencer;
    }

    /**
     * 
     * @return The candidates for the leader election.
     */
    public Map<Integer, Integer> getCandidates() {
        return this.candidates;
    }

    /**
     * Custom hashcode for messsage.
     * 
     * @return Unique hashcode of message.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uid.toString() == null) ? 0 : uid.toString().hashCode());
        result = prime * result + (int) (this.initialSender ^ (this.initialSender >>> 32));
        return result;
    }
}
