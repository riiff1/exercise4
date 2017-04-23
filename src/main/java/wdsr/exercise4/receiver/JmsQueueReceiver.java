package wdsr.exercise4.receiver;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;
import wdsr.exercise4.sender.JmsSender;

import javax.jms.*;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver implements ExceptionListener {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	private final String MESSAGE_TYPE_PRICE = "PriceAlert";
	private final String MESSAGE_TYPE_VOLUME = "VolumeAlert";
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private Destination destinationQue;
	private MessageConsumer messageConsumerQue;
	private AlertService alertService;
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) {
		// TODO
		connectionFactory = new ActiveMQConnectionFactory("vm://localhost:61616");
		try {
			connectionFactory.setTrustAllPackages(true); // czy mozna jakos ograniczyc te pakiety?
			connection = connectionFactory.createConnection();
			connection.start();
			connection.setExceptionListener(this);
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			destinationQue = session.createQueue(queueName);

			messageConsumerQue = session.createConsumer(destinationQue);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
		// TODO
		try {
			this.alertService = alertService;
			messageConsumerQue.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
					try {
						String messageType = message.getJMSType().toString();
						if(message instanceof TextMessage) {
							textMessageConsumer(messageType, message);
						} else if(message instanceof ObjectMessage) {
							objectMessageConsumer(messageType, message);
						} else {
							log.info("Different message");
						}
					} catch (JMSException e) {
						e.printStackTrace();
					}
                }
            });
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void objectMessageConsumer(String messageType, Message message) {
		try {
			ObjectMessage objectMessage = (ObjectMessage) message;
			if (messageType.equals(MESSAGE_TYPE_PRICE)) {
				PriceAlert priceAlert = (PriceAlert) objectMessage.getObject();
				alertService.processPriceAlert(priceAlert);
			} else if (messageType.equals(MESSAGE_TYPE_VOLUME)) {
				VolumeAlert volumeAlert = (VolumeAlert) objectMessage.getObject();
				alertService.processVolumeAlert(volumeAlert);
			} else {
				log.info("Different JMS type.");
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void textMessageConsumer(String messageType, Message message) {
		try {
			if (messageType.equals(MESSAGE_TYPE_PRICE)) {
				TextMessage textMessage = (TextMessage) message;
				ArrayList<String> priceValues = getSplitTabFromTextMessage(textMessage.getText());
				PriceAlert priceAlert = new PriceAlert(new Long(priceValues.get(0)), priceValues.get(1), new BigDecimal(priceValues.get(2).trim()));
				alertService.processPriceAlert(priceAlert);
			} else if (messageType.equals(MESSAGE_TYPE_VOLUME)) {
				TextMessage textMessage = (TextMessage) message;
				ArrayList<String> priceValues = getSplitTabFromTextMessage(textMessage.getText());
				VolumeAlert volumeAlert = new VolumeAlert(new Long(priceValues.get(0)), priceValues.get(1), new Long(priceValues.get(2).trim()));
				alertService.processVolumeAlert(volumeAlert);
			} else {
				log.info("Different JMS type.");
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		// TODO
		try {
			messageConsumerQue.close();
			session.close();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onException(JMSException exception) {
		log.info("JMS Exception occured.  Shutting down client.");
	}

	private ArrayList<String> getSplitTabFromTextMessage(String messageText) {
		String[] split = messageText.split("\n");
		ArrayList<String> values = new ArrayList<>(3);
		for(String s: split) {
			String[] tmp = s.split("=");
			values.add(tmp[1]);
		}
		return values;
	}

	// TODO
	// This object should start consuming messages when registerCallback method is invoked.
	
	// This object should consume two types of messages:
	// 1. Price alert - identified by header JMSType=PriceAlert - should invoke AlertService::processPriceAlert
	// 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke AlertService::processVolumeAlert
	// Use different message listeners for and a JMS selector 
	
	// Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert or VolumeAlert class)
	// or as a TextMessage.
	// Text for PriceAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Price=<long value>
	// Text for VolumeAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Volume=<long value>
	
	// When shutdown() method is invoked on this object it should remove the listeners and close open connection to the broker.
}
