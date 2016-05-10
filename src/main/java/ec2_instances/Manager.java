package ec2_instances;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.hp.gagawa.java.elements.Body;
import com.hp.gagawa.java.elements.Font;
import com.hp.gagawa.java.elements.Head;
import com.hp.gagawa.java.elements.Html;
import com.hp.gagawa.java.elements.P;
import com.hp.gagawa.java.elements.Text;
import com.hp.gagawa.java.elements.Title;

import ass1.amazon_utils.EC2LaunchFactory;
import ass1.amazon_utils.Ec2InstanceType;
import ass1.amazon_utils.S3Handler;
import ass1.amazon_utils.SQSservice;
import local_application.TweetAnalysisOutput;

public class Manager {
	private static String accKey = "AKIAJIWZRPBTMYXF4K7Q";
	private static String secKey = "";
	private static String localToManagerqueueName = "localAppToManager";
	private static String jobsQueue = "jobsQueue";
	private static String resultsQueue = "resultsQueue";

	public static void main(String[] args) throws InterruptedException {
		System.out.println("Manager Service Invocated!");

		// Init Utils and Queues.
		 Queue<String> jobs = new LinkedList<String>();
		int messagesPerWorker = 0, numOfRequiredWorkers = 0, numOfTweets = 0, numOfActiveWorkers = 0;
		AWSCredentials credentials = setCredentialsFromArgs(accKey, secKey);
		SQSservice mySqsService = new SQSservice(credentials);
		AmazonEC2Client ec2Client = new AmazonEC2Client(credentials);
		EC2LaunchFactory ec2Factory = new EC2LaunchFactory(credentials);
		ec2Client.setEndpoint("ec2.us-east-1.amazonaws.com");
		S3Handler s3Hander = new S3Handler(credentials);
		AmazonSQSClient sqsClient = new AmazonSQSClient(credentials);
		String localAppToManagerQueueUrl = sqsClient.getQueueUrl(localToManagerqueueName).getQueueUrl();
		//List<Message> messages = mySqsService.recieveMessages(localToManagerqueueName, localAppToManagerQueueUrl);
		LocalAppToManagerListener listener = new LocalAppToManagerListener(mySqsService, jobs,
				localAppToManagerQueueUrl);
		Thread listenerThread = new Thread(listener);
		listenerThread.start();;
		System.out.println("Sent Thread..");
		String currJOb;
		while (true) {
			currJOb = jobs.poll();
			System.out.println("Manager: curr job is:" + currJOb);
			if (currJOb != null) {
				System.out.println("Manager: got a new Message:\n" + currJOb);
				if (currJOb.equals("STOP!")){
					break;
				}
				//Got a job..
				try {
//					String inputFileArgs = getInputFileUrlAndDeleteMessage(messages, mySqsService,
//							localAppToManagerQueueUrl);
					String[] splitArgs = currJOb.split("\\r?\\n");
					messagesPerWorker = Integer.valueOf(splitArgs[1]);
					System.out.println("Manager: Recieved new Job\nMessagesPerWorker = " + splitArgs[1]);
				} catch (Exception e) {
					System.out.println(e);
				}
				// download tweets file and send links on jobs Queue.
				//TODO: download by url..
				S3Object tweetsObject = s3Hander.downloadFile("Tweets");
				//TODO: check if not exist.
				String jobsQueueUrl = mySqsService.createQueue(jobsQueue);
				String resultsQueueUrl = mySqsService.createQueue(resultsQueue);
				try {
					numOfTweets = distributeMessagesAndInsertToQueue(tweetsObject.getObjectContent(), jobsQueueUrl,
							mySqsService);
				} catch (Exception e) {
					System.out.println("Error reading from Downloaded file.\n" + e);
				}
				numOfRequiredWorkers = numOfTweets / messagesPerWorker;
				numOfActiveWorkers = countTypeAppearances(ec2Client, Ec2InstanceType.WORKER);
				numOfRequiredWorkers -= numOfActiveWorkers;
				// Creating Workers:
				System.out.println("Number of required workers:" + numOfRequiredWorkers);
				System.out.println("Starting to create workers..");
				for (int i = 0; i < numOfRequiredWorkers; i++) {
					System.out.println(String.format("Creating worker number %d!", i + 1));
					ec2Factory.launchEC2Instance(Ec2InstanceType.WORKER);
				}

				List<TweetAnalysisOutput> tweetsAnalysisOutputs = Lists.newArrayList();
				// waiting for workers to complete jobs.
				while (tweetsAnalysisOutputs.size() < numOfTweets) {
					List<Message> currBatch = mySqsService.recieveMessages(resultsQueue, resultsQueueUrl);
					System.out.println("got " + tweetsAnalysisOutputs.size() + "Messages so far..");
					extractTweetOutputsFromMessages(currBatch, tweetsAnalysisOutputs);
					deleteMessagesFromQueue(currBatch, mySqsService, resultsQueueUrl);
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println();
				String html = buildHtmlString(tweetsAnalysisOutputs);
				System.out.println("Sending HTML String to LocalApp->Manager Queue.");
				mySqsService.sendMessage(html, localToManagerqueueName, localAppToManagerQueueUrl);

			}

			else {
				Thread.sleep(1000);
			}
		}
		System.out.println("Recieved Quit command..\nExiting.");
	}

	private static void deleteMessagesFromQueue(List<Message> currBatch, SQSservice mySqsService, String queueUrl) {
		for (Message message : currBatch) {
			mySqsService.deleteMessage(message, queueUrl);
		}

	}

	private static List<TweetAnalysisOutput> processRawOutput(String rawOutputsAsSrting) {
		ObjectMapper om = new ObjectMapper();
		List<TweetAnalysisOutput> processedTweets = Lists.newArrayList();
		try {
			List<TweetAnalysisOutput> tweetOutputobj = om.readValue(rawOutputsAsSrting,
					new TypeReference<List<TweetAnalysisOutput>>() {
					});
			System.out.println("Desiarilzed: " + tweetOutputobj);
			processedTweets.addAll(tweetOutputobj);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return processedTweets;
	}

	private static void extractTweetOutputsFromMessages(final List<Message> currBatch,
			List<TweetAnalysisOutput> tweetsOutputs) {
		if (currBatch != null) {
			for (Message message : currBatch) {
				try {
					String extracted = message.getBody();
					System.out.println("tweet outputs was " + tweetsOutputs.size());
					tweetsOutputs.addAll(processRawOutput(extracted));
					System.out.println("tweet outputs Is now" + tweetsOutputs.size());
				} catch (Exception e) {
					System.out.println("Caught Exception trying to parse a message from queue..");
					System.out.println(e);
				}
			}
		}
	}

	public static AWSCredentials setCredentialsFromArgs(String accKey, String seckey) {
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials(accKey, seckey);
		} catch (Exception e) {
			throw new AmazonClientException("credentials given fail to log ...", e);
		}
		return credentials;

	}

	public static String getInputFileUrlAndDeleteMessage(List<Message> messages, SQSservice mySqsService,
			String queueUrl) throws Exception {
		String url = null;
		Message m = null;
		for (Message message : messages) {
			if (message.getBody().contains("Tweets")) {
				url = message.getBody();
				m = message;
				break;
			}
		}
		if (m == null) {
			throw new Exception("Input File Url Missing from localAppToManager queue");
		}
		mySqsService.deleteMessage(m, queueUrl);
		return url;
	}

	private static int distributeMessagesAndInsertToQueue(InputStream input, String jobsQueueUrl, SQSservice sqsService)
			throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		int numOTweets = 0;
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;
			sqsService.sendMessage(line, jobsQueue, jobsQueueUrl);
			numOTweets++;
		}
		System.out.println();
		return numOTweets;
	}

	private static int countTypeAppearances(AmazonEC2Client ec2Client, Ec2InstanceType type) {
		int numOfActiveWorkers = 0;
		List<Reservation> reservations = ec2Client.describeInstances().getReservations();
		List<Instance> instances = null;
		for (Reservation resv : reservations) {
			instances = resv.getInstances();
			for (Instance instance : instances) {
				List<Tag> tags = instance.getTags();
				for (Tag tag : tags) {
					if (tag.getKey().equals("Type") && tag.getValue().equals(type.name())) {
						if (instance.getState().getName().equals("running")) {
							numOfActiveWorkers++;
						}
					}
				}
			}
		}
		return numOfActiveWorkers;
	}

	public static List<String> getResultsFromResultsQueue(SQSservice mySqsService, String resultsQueueUrl) {
		List<String> results = Lists.newArrayList();
		List<Message> messages = mySqsService.recieveMessages(resultsQueue, resultsQueueUrl);
		for (Message resultMessage : messages) {
			results.add(resultMessage.getBody());
			System.out.println("result Id" + resultMessage.getMessageId() + " Link:\n" + resultMessage.getBody()
					+ "\nTaken from queue");
		}
		for (Message resultMessageToDelete : messages) {
			mySqsService.deleteMessage(resultMessageToDelete, resultsQueueUrl);
			System.out.println("result Id" + resultMessageToDelete.getMessageId() + "\nDeleted!");
		}
		return results;
	}

	private static String buildHtmlString(List<TweetAnalysisOutput> outputs) {
		System.out.println("building html..");
		Html html = new Html();
		Head head = new Head();
		// keep the head?
		html.appendChild(head);
		Title title = new Title();
		title.appendChild(new Text("Tweet Analysis Output!"));
		head.appendChild(new Text("### Tweet Analysis Output ! ###"));
		head.appendChild(title);
		Body body = new Body();
		html.appendChild(body);
		for (TweetAnalysisOutput tweetOutput : outputs) {
			P p = new P();
			Font f = new Font();
			f.setColor(getColorFromScore(tweetOutput.getScore()));
			f.appendChild(new Text(tweetOutput.getTweet()));
			p.appendChild(f);
			p.appendChild(new Text("  " + tweetOutput.getSentiments().toString()));

			body.appendChild(p);

		}
		System.out.println("Finished building html..");
		return html.write();
	}

	private static String getColorFromScore(int score) {
		String color;
		switch (score) {
		case 0:
			color = "darkred";
			break;
		case 1:
			color = "red";
			break;
		case 2:
			color = "black";
			break;
		case 3:
			color = "lightgreen";
			break;
		case 4:
			color = "darkgreen";
			break;
		default:
			color = "";
			break;
		}

		return color;
	}
}
