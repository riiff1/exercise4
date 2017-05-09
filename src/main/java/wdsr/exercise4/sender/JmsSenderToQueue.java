package wdsr.exercise4.sender;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * Created by Rafal on 4/24/2017.
 */
public class JmsSenderToQueue {
    private static final Logger log = LoggerFactory.getLogger(JmsSenderToQueue.class);
    private String queueName;
    private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destinationQue;
    private MessageProducer messageProducerQue;
    private boolean persistent;

    public JmsSenderToQueue(String queueName, boolean persistent) {
        this.queueName = queueName;
        this.persistent = persistent;
        connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destinationQue = session.createQueue(this.queueName);

            messageProducerQue = session.createProducer(destinationQue);
            if(persistent) {
                messageProducerQue.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
                messageProducerQue.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void sendMessageToQueue() {
        log.info("Persistent: " + persistent);
        long start=System.currentTimeMillis();
        log.info("Wysylanie wiadomosci - START " + start);
        //nie wiem czy chcial pan liczenie dwoch traszy pierwszy od 1 do 10k i drugi od 1 do 10k.
        //zrobilem to ze pierwszy idzie od 1 do 10k a pozniej numeracja jest od 10001 do 20k
        if(persistent) {
            for(int i=1; i<= 10000;i++) {
                try {
                    String text = "test_"+i;
                    TextMessage textMessage = session.createTextMessage(text);
                    messageProducerQue.send(textMessage);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            for(int i=10001; i<= 20000;i++) {
                try {
                    String text = "test_"+i;
                    TextMessage textMessage = session.createTextMessage(text);
                    messageProducerQue.send(textMessage);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
        long stop=System.currentTimeMillis();
        log.info("Wysylanie wiadomosci - STOP " + stop);
        log.info("Czas wykonywania programu: " + (stop-start) + " milisekund");
    }

    public void closeAllConnections() {
        try {
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }
}
