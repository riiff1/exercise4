package wdsr.exercise4.receiver;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.sender.JmsSenderToQueue;

import javax.jms.*;
import javax.xml.soap.Text;
import java.util.ArrayList;

/**
 * Created by Rafal on 5/9/2017.
 */
public class JmsReceiverFromQueue {
    private static final Logger log = LoggerFactory.getLogger(JmsReceiverFromQueue.class);
    private String queueName;
    private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destinationQue;
    private MessageConsumer messageConsumerQue;
    private ArrayList<String> messages = new ArrayList<>(20000);

    public JmsReceiverFromQueue(String queueName) {
        this.queueName = queueName;
        connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        try {
            connectionFactory.setTrustAllPackages(true);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destinationQue = session.createQueue(this.queueName);

            messageConsumerQue = session.createConsumer(destinationQue);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void getMessageFromQueue() {
        try {
            if(messageConsumerQue != null)
            {
                Message message = messageConsumerQue.receive(Message.DEFAULT_DELIVERY_DELAY);
                while(message != null) {
                    if(message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        messages.add(textMessage.getText());
                        System.out.println(textMessage.getText());
                    }
                    message = messageConsumerQue.receive(100);
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
