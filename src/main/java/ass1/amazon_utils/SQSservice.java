package ass1.amazon_utils;

import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSservice {
	private AmazonSQS sqsClient;

	public SQSservice(AWSCredentials credentials) {
		this.sqsClient = new AmazonSQSClient(credentials);
	}

	public String createQueue(String queueName) {

		System.out.println("===========================================");
		System.out.println("Creating Amazon SQS ");
		System.out.println("===========================================\n");
		String myQueueUrl = null;
		try {
			// Create a queue
			System.out.println("Creating a new SQS queue called " + queueName
					+ ".\n");
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(
					queueName);
			myQueueUrl = this.sqsClient.createQueue(createQueueRequest)
					.getQueueUrl();

			// List queues
			System.out.println("Listing all queues in your account.\n");
			for (String queueUrl : this.sqsClient.listQueues().getQueueUrls()) {
				System.out.println("  QueueUrl: " + queueUrl);
			}
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return myQueueUrl;
	}

	public void sendMessage(String message, String queueName, String myQueueUrl) {
		// Send a message
		System.out.println("Sending a message to " + queueName + ".\n");
		try {
			this.sqsClient.sendMessage(new SendMessageRequest(myQueueUrl,
					message));
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	public List<Message> recieveMessages(String queueName, String myQueueUrl) {
		// Receive messages
		List<Message> messages = null;
		try {
			System.out.println("Receiving messages from" + queueName + ".\n");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
					myQueueUrl).withMaxNumberOfMessages(10);
			messages = this.sqsClient.receiveMessage(receiveMessageRequest)
					.getMessages();
			for (Message message : messages) {
				System.out.println("  Message");
				System.out.println("    MessageId:     "
						+ message.getMessageId());
				System.out.println("    ReceiptHandle: "
						+ message.getReceiptHandle());
				System.out.println("    MD5OfBody:     "
						+ message.getMD5OfBody());
				System.out.println("    Body:          " + message.getBody());
				for (Entry<String, String> entry : message.getAttributes()
						.entrySet()) {
					System.out.println("  Attribute");
					System.out.println("    Name:  " + entry.getKey());
					System.out.println("    Value: " + entry.getValue());
				}
			}
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return messages;
	}

	public void deleteMessage(Message message, String myQueueUrl) {
		// Delete a message
		System.out.println("Deleting a message.\n");
		try {
			String messageRecieptHandle = message.getReceiptHandle();
			this.sqsClient.deleteMessage(new DeleteMessageRequest(myQueueUrl,
					messageRecieptHandle));
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	public void deleteQueue(String queueName, String myQueueUrl) {
		// Delete a queue
		System.out.println("Deleting queue" + ".\n");
		try {
			this.sqsClient.deleteQueue(new DeleteQueueRequest(myQueueUrl));
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

}
