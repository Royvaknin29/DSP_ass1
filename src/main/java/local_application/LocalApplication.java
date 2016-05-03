package local_application;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.sqs.model.Message;

import ass1.amazon_utils.EC2LaunchFactory;
import ass1.amazon_utils.Ec2InstanceType;
import ass1.amazon_utils.S3Handler;
import ass1.amazon_utils.SQSservice;

public class LocalApplication {
	private String LOCAL_APP_TO_MANAGER = "localAppToManager";
	private AWSCredentials credentials;
	private EC2LaunchFactory EC2Factory;
	private S3Handler s3Handler;
	private SQSservice sqsService;
	private AmazonEC2Client ec2Client;
	private String outputFilename;
	
	public LocalApplication(AWSCredentials credentials, String outputFilename) {
		this.credentials = credentials;
		initializeAmazonUtils();
		this.outputFilename = outputFilename;
	}

	public void startApplication(String[] inputVars) {
		System.out.println(
				"=======================================\nWelcome to the Tweet Analyser!!!\n=======================================");

		if (!checkIfTypeExists(ec2Client, Ec2InstanceType.MANAGER)) {
			System.out.println("Creating new Manager!");
			this.EC2Factory.launchEC2Instance(Ec2InstanceType.MANAGER);
		}
		this.s3Handler.createBucket();
		String inputLocation = this.s3Handler.uploadFile(inputVars[0], "Tweets");
		System.out.println("Input file uploaded to:\n" + inputLocation);
		String localAppToManagersqsUrl = sqsService.createQueue(LOCAL_APP_TO_MANAGER);
		sqsService.sendMessage(inputLocation + "\n" + inputVars[2], LOCAL_APP_TO_MANAGER, localAppToManagersqsUrl);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		List<Message> htmlStringMessage = sqsService.recieveMessages(LOCAL_APP_TO_MANAGER, localAppToManagersqsUrl);
		String htmlString = htmlStringMessage.get(0).getBody();
		try {
			writeToHtmlFile(htmlString);
		} catch (FileNotFoundException e) {
			System.out.println("Failed writing html string to file");
			e.printStackTrace();
		}
	}

	private void initializeAmazonUtils() {
		System.out.println("Initializing Amazon Utils");
		this.EC2Factory = new EC2LaunchFactory(this.credentials);
		this.s3Handler = new S3Handler(this.credentials);
		this.sqsService = new SQSservice(this.credentials);
		this.ec2Client = new AmazonEC2Client(this.credentials);
		ec2Client.setEndpoint("ec2.us-east-1.amazonaws.com");

	}

	private boolean checkIfTypeExists(AmazonEC2Client ec2Client, Ec2InstanceType type) {
		List<Reservation> reservations = ec2Client.describeInstances().getReservations();
		List<Instance> instances = null;
		for (Reservation resv : reservations) {
			instances = resv.getInstances();
			for (Instance instance : instances) {
				List<Tag> tags = instance.getTags();
				for (Tag tag : tags) {
					if (tag.getKey().equals("Type") && tag.getValue().equals(type.name())) {
						if (instance.getState().getName().equals("running")) {
							System.out.println("Found active manager!!");
							return true;
						}
					}
				}
			}
		}
		return false;
	}
	
	private void writeToHtmlFile(String html) throws FileNotFoundException{
		
		try(  PrintWriter out = new PrintWriter( this.outputFilename + ".html" )  ){
		    out.println( html );
		}
	}
	



}
