package wdsr.exercise4;

import wdsr.exercise4.sender.JmsSenderToQueue;

/**
 * Created by Rafal on 4/23/2017.
 */
public class Main {
    public static void main(String[] args) {
        JmsSenderToQueue jmsSenderToQueue = new JmsSenderToQueue("testQueue", false);
        jmsSenderToQueue.sendMessageToQueue();
    }
}
