package local_application;

import java.util.List;

import ass1.amazon_utils.SQSservice;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.model.Message;

public class Testing {
	private static String accKey = "";
	private static String secKey = "";
	public static void main(String[] args) {
		AWSCredentials credentials = setCredentialsFromArgs(accKey, secKey);
		SQSservice sqsService = new SQSservice(credentials);
		//sqsService.createQueue("TEST_QUEUE");
		//sqsService.sendMessage("this is a test message", "TEST_QUEUE", "https://sqs.us-east-1.amazonaws.com/854814600256/TEST_QUEUE");
		List<Message> messages = sqsService.recieveMessages("TEST_QUEUE", "https://sqs.us-east-1.amazonaws.com/854814600256/TEST_QUEUE");
		System.out.println(messages.get(0));
		sqsService.deleteMessage(messages.get(0), "https://sqs.us-east-1.amazonaws.com/854814600256/TEST_QUEUE");
	
	}
	public static AWSCredentials setCredentialsFromArgs(String accKey, String seckey) {
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials(accKey, seckey);
		} catch (Exception e) {
			throw new AmazonClientException(
					"credentials given fail to log ...", e);
		}
		return credentials;

	}
}
