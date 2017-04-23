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

    public JmsSenderToQueue(String queueName, boolean persistent) {
        this.queueName = queueName;
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
        for(int i=1; i<= 10000;i++) {
            try {
                String text = "test_"+i;
                TextMessage textMessage = session.createTextMessage(text);
                messageProducerQue.send(textMessage);
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
