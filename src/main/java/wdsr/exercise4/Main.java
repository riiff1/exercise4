package wdsr.exercise4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.receiver.JmsReceiverFromQueue;
import wdsr.exercise4.receiver.JmsTopicSubscriber;
import wdsr.exercise4.sender.JmsSenderToQueue;

/**
 * Created by Rafal on 4/23/2017.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        JmsTopicSubscriber jmsTopicSubscriber =  new JmsTopicSubscriber("RIIFF1.TOPIC");
        jmsTopicSubscriber.getMessageFromTopic();
        jmsTopicSubscriber.closeAllConnections();
        System.out.println(jmsTopicSubscriber.getMessages().size());
    }
}
