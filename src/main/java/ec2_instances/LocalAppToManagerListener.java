package ec2_instances;

import java.util.List;
import java.util.Queue;

import com.amazonaws.services.sqs.model.Message;

import ass1.amazon_utils.SQSservice;

public class LocalAppToManagerListener implements Runnable {
	private static String localToManagerqueueName = "localAppToManager";
	private SQSservice mySqsService;
	private Queue<String> jobs;
	private String localAppToManagerQueueUrl;

	public LocalAppToManagerListener(SQSservice mySqsService, Queue<String> jobs, String localAppToManagerQueueUrl) {
		this.jobs = jobs;
		this.mySqsService = mySqsService;
		this.localAppToManagerQueueUrl = localAppToManagerQueueUrl;
	}

	@Override
	public void run() {
		while (true) {
			List<Message> messages = mySqsService.recieveMessages(localToManagerqueueName, localAppToManagerQueueUrl);
			if (messages.size() > 0) {
				System.out.println("Listener: Got message from localApp");
				analyzeAndDeleteMessages(messages);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	private void analyzeAndDeleteMessages(List<Message> messages) {
		for (Message message : messages) {
			if (message.getBody().substring(0, 5).equals("https")) {
				System.out.println("Listener: Got a new Job!");
				jobs.add(message.getBody());
				System.out.println("Listener: job added to queue..Queue size is now:" + jobs.size());
			} else if (message.getBody().length() < 6) {
				jobs.add("STOP!");
			}
		}
		deleteMessagesFromQueue(messages);
	}

	private void deleteMessagesFromQueue(List<Message> currBatch) {
		for (Message message : currBatch) {
			if (message.getBody().contains("Analysis Output")) {
				continue;
			} else {
				this.mySqsService.deleteMessage(message, this.localAppToManagerQueueUrl);
			}
		}
	}
}
