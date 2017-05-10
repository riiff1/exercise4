package wdsr.exercise4.receiver;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;

/**
 * Created by Rafal on 5/10/2017.
 */
public class JmsTopicSubscriber {
    private static final Logger log = LoggerFactory.getLogger(JmsReceiverFromQueue.class);
    private String topicName;
    private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destinationTopic;
    private MessageConsumer messageConsumerTopic;
    private ArrayList<String> messages = new ArrayList<>(20000);

    public JmsTopicSubscriber(String topicName) {
        this.topicName = topicName;
        connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        try {
            connectionFactory.setTrustAllPackages(true);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destinationTopic = session.createQueue(this.topicName);

            messageConsumerTopic = session.createConsumer(destinationTopic);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void getMessageFromQueue() {
        try {
            if(messageConsumerTopic != null)
            {
                Message message = messageConsumerTopic.receive(Message.DEFAULT_DELIVERY_DELAY);
                while(message != null) {
                    if(message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        messages.add(textMessage.getText());
                        System.out.println(textMessage.getText());
                    }
                    message = messageConsumerTopic.receive(100);
                }
            }


        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void closeAllConnections() {
        try {
            if(session != null && connection != null) {
                session.close();
                connection.close();
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    public ArrayList<String> getMessages() {
        return messages;
    }
}
