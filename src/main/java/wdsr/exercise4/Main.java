package wdsr.exercise4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.receiver.JmsReceiverFromQueue;
import wdsr.exercise4.sender.JmsSenderToQueue;
import wdsr.exercise4.sender.JmsSenderToTopic;

/**
 * Created by Rafal on 4/23/2017.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        JmsSenderToTopic jmsSenderToTopic = new JmsSenderToTopic("RIIFF1.TOPIC", false);
        jmsSenderToTopic.sendMessageToTopic();
        jmsSenderToTopic.closeAllConnections();
        JmsSenderToTopic jmsSenderToTopic2 = new JmsSenderToTopic("RIIFF1.TOPIC", true);
        jmsSenderToTopic2.sendMessageToTopic();
        jmsSenderToTopic2.closeAllConnections();
    }
}
