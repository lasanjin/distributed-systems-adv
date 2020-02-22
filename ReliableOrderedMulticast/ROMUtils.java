import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Helper functions for Reliable Ordered Multicaster.
 */
public class ROMUtils {

    private ROMUtils() {
    }

    /**
     * Build ring topology based on hosts from Multicaster because we don't have
     * access to anything else.
     * 
     * @param hosts The number of hosts.
     * @return Node Ids mapped to cpu capacity OR local sequence number.
     */
    public static Map<Integer, Integer> buildRingTop(int hosts) {
        Map<Integer, Integer> nodes = new HashMap<>();

        for (int i = 0; i < hosts; i++) {
            nodes.put(i, Integer.MIN_VALUE);
        }

        return nodes;
    }

    /**
     * Broadcast custom message to all nodes except self
     * 
     * @param bcom The communication backend.
     * @param rom  Reliable Ordered Multicaster.
     * @param msg  The message to broadcast.
     */
    public static void broadcast(mcgui.BasicCommunicator bcom, ROM rom, ROMMessage msg) {
        for (Integer nodeId : rom.getNodes().keySet()) {
            /* Send to everyone except self */
            if (nodeId != rom.getId()) {
                bcom.basicsend(nodeId, msg);
            }
        }
    }

    /**
     * Sends queued messages to sequencer.
     *
     * @param bcom The communication backend.
     * @param rom  Reliable Ordered Multicaster.
     */
    public static void sendQueuedMessages(mcgui.BasicCommunicator bcom, ROM rom) {
        Iterator<ROMMessage> q = rom.getQueuedMessages().iterator();

        while (q.hasNext()) {
            bcom.basicsend(rom.getSequencer(), q.next());
        }
    }

    /**
     * Removes queued message from initial sender of message.
     * 
     * @param rom Reliable Ordered Multicaster.
     * @param msg The message from sender to remove from queue.
     */
    public static void removeQueuedMessages(ROM rom, ROMMessage msg) {
        /* Remove message if it was sent it */
        if (rom.getId() == msg.getInitialSender()) {
            rom.removeQueuedMessage(msg);
        }
    }

    /**
     * Looks for any pending messages from sender that have not been delivered yet.
     * 
     * @param rom    Reliable Ordered Multicaster.
     * @param sender The intial sender of the message.
     */
    public static void deliverPendingMessages(ROM rom, int sender) {

        for (ROMMessage msg : rom.getPendingMessages()) {
            /*
             * If pending message is from sender and it is the next message expected from
             * sender
             */
            if (msg.getInitialSender() == sender
                    && msg.getMessageNum().equals(rom.getNextMessage().getOrDefault(sender, 0) + 1)) {
                rom.receiveMessage(msg);
            }
        }
    }

    /**
     * Broadcasts message to all nodes except self and sequencer.
     *
     * @param bcom The communication backend.
     * @param rom  Reliable Ordered Multicaster.
     * @param msg  The message to broadcast.
     */
    public static void multicastDeliveredMessage(mcgui.BasicCommunicator bcom, ROM rom, ROMMessage msg) {
        for (Integer nodeId : rom.getNodes().keySet()) {

            if (nodeId != rom.getId() && nodeId != rom.getSequencer()) {
                bcom.basicsend(nodeId, msg);
            }
        }
    }
}