package ec2_instances;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;

import ass1.amazon_utils.Ec2InstanceType;
import ass1.amazon_utils.S3Handler;
import ass1.amazon_utils.SQSservice;

public class Manager {
	private static String accKey = "AKIAJ7NENWCNH4ZIBIQQ";
	private static String secKey = "LWce1dJ65wK2ZCMYPTL+vnVLwBPMPh5fvNbxhnOC";
	private static String localToManagerqueueName = "localAppToManager";
	private static String jobsQueue = "jobsQueue";
	private static String resultsQueue = "resultsQueue";
	
	public static void main(String[] args) {
		System.out.println("Manager Service Invocated!");
		//Init Utils and Queues.
		int messagesPerWorker = 0, numOfRequiredWorkers = 0, numOfTweets = 0, numOfActiveWorkers = 0;
		AWSCredentials credentials = setCredentialsFromArgs(accKey, secKey);
		SQSservice mySqsService = new SQSservice(credentials);
		AmazonEC2Client ec2Client = new AmazonEC2Client(credentials);
		ec2Client.setEndpoint("ec2.us-east-1.amazonaws.com");
		S3Handler s3Hander = new S3Handler(credentials);
		AmazonSQSClient sqsClient = new AmazonSQSClient(credentials);
		String localAppToManagerQueueUrl = sqsClient.getQueueUrl(localToManagerqueueName)
				.getQueueUrl();
		List<Message> messages = mySqsService.recieveMessages(localToManagerqueueName,
				localAppToManagerQueueUrl);
		//get args from localApp
		try {
			String inputFileArgs = getInputFileUrlAndDeleteMessage(messages, mySqsService,
					localAppToManagerQueueUrl);
			String[] splitArgs = inputFileArgs.split("\\r?\\n");
			messagesPerWorker = Integer.valueOf(splitArgs[1]);
			System.out.println("Recieved URL: " + inputFileArgs+ "\nMessagesPerWorker = " + splitArgs[1]);
		} catch (Exception e) {
			System.out.println(e);
		}
		//download tweets file and send links on jobs Queue.
		S3Object tweetsObject = s3Hander.downloadFile("Tweets");
		String jobsQueueUrl = mySqsService.createQueue(jobsQueue);
		String resultsQueueUrl = mySqsService.createQueue(resultsQueue);
		try {
			numOfTweets = distibuteMessagesAndInsertToQueue(tweetsObject.getObjectContent(), jobsQueueUrl, mySqsService);
		} catch (IOException e) {
			System.out.println("Error reading from Downloaded file.\n" + e);
		}
		numOfRequiredWorkers = numOfTweets / messagesPerWorker;
		numOfActiveWorkers = countTypeAppearances(ec2Client, Ec2InstanceType.WORKER);
		numOfRequiredWorkers -= numOfActiveWorkers;
		//for(numOfRequiredWorkers); CREATE WORKERS! 
	
	}

	public static AWSCredentials setCredentialsFromArgs(String accKey,
			String seckey) {
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials(accKey, seckey);
		} catch (Exception e) {
			throw new AmazonClientException(
					"credentials given fail to log ...", e);
		}
		return credentials;

	}

	public static String getInputFileUrlAndDeleteMessage(List<Message> messages,
			SQSservice mySqsService, String queueUrl) throws Exception {
		String url = null;
		Message m = null;
		for (Message message : messages) {
			if (message.getBody().contains(".txt")) {
				url = message.getBody();
				m = message;
				break;
			}
		}
		if (m == null) {
			throw new Exception(
					"Input File Url Missing from localAppToManager queue");
		}
		mySqsService.deleteMessage(m, queueUrl);
		return url;
	}

	 private static int distibuteMessagesAndInsertToQueue(InputStream input, String jobsQueueUrl, SQSservice sqsService) throws IOException {
	        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	        int numOTweets = 0;
	        while (true) {
	            String line = reader.readLine();
	            if (line == null) break;
	            sqsService.sendMessage(line, jobsQueue, jobsQueueUrl);
	            numOTweets++;
	        }
	        System.out.println();
	 return numOTweets;   
	 }
		private static int countTypeAppearances(AmazonEC2Client ec2Client,
				Ec2InstanceType type) {
			int numOfActiveWorkers = 0;
			List<Reservation> reservations = ec2Client.describeInstances()
					.getReservations();
			List<Instance> instances = null;
			for (Reservation resv : reservations) {
				instances = resv.getInstances();
				for (Instance instance : instances) {
					List<Tag> tags = instance.getTags();
					for (Tag tag : tags) {
						if (tag.getKey().equals("Type")
								&& tag.getValue().equals(type.name())) {
							if (instance.getState().getName().equals("running")) {
								numOfActiveWorkers++;
							}
						}
					}
				}
			}
			return numOfActiveWorkers;
		}
}
