package wdsr.exercise4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.sender.JmsSenderToQueue;

/**
 * Created by Rafal on 4/23/2017.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        JmsSenderToQueue jmsSenderToQueue = new JmsSenderToQueue("RIIFF1.QUEUE", false);
        jmsSenderToQueue.sendMessageToQueue();
        jmsSenderToQueue.closeAllConnections();
        log.info("");
        JmsSenderToQueue jmsSenderToQueue2 = new JmsSenderToQueue("RIIFF1.QUEUE", true);
        jmsSenderToQueue2.sendMessageToQueue();
        jmsSenderToQueue2.closeAllConnections();
    }
}
