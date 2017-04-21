package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.Order;

import javax.jms.*;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private Destination destination;
	private MessageProducer messageProducer;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		connectionFactory = new ActiveMQConnectionFactory("vm://localhost:61616");
		try {
			connection = connectionFactory.createConnection();
			connection.start();

			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			destination = session.createQueue(queueName);

			messageProducer = session.createProducer(destination);
			messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		// TODO
		Order order = new Order(orderId, product, price);
		try {
			ObjectMessage objectMessage = session.createObjectMessage(order);
			objectMessage.setJMSType("Order");
			//po co tak na prawde ustawiamy property dla Message? jesli by mi testy na tym nie failowaly to bym np nie wiedzial ze musze to ustawic
			objectMessage.setStringProperty("WDSR-System", "OrderProcessor");
			messageProducer.send(objectMessage);
			session.close();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		// TODO
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		// TODO
	}
}
