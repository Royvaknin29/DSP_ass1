package local_application;

import java.util.List;

import ass1.amazon_utils.EC2LaunchFactory;
import ass1.amazon_utils.Ec2InstanceType;
import ass1.amazon_utils.S3Handler;
import ass1.amazon_utils.SQSservice;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;

public class LocalApplication {
	private String LOCAL_APP_TO_MANAGER = "localAppToManager";
	private AWSCredentials credentials;
	private EC2LaunchFactory EC2Factory;
	private S3Handler s3Handler;
	private SQSservice sqsService;
	private AmazonEC2Client ec2Client;
	public LocalApplication(AWSCredentials credentials) {
		this.credentials = credentials;
		initializeAmazonUtils();
	}

	public void startApplication(String[] inputVars) {
		int urlsPerWorker;
		System.out
				.println("=======================================\nWelcome to the Tweet Analyser!!!\n=======================================");

		urlsPerWorker = Integer.valueOf(inputVars[2]);
		

		if (!checkIfTypeExists(ec2Client, Ec2InstanceType.MANAGER)){
			System.out.println("Creating new Manager!");
			this.EC2Factory.launchEC2Instance(Ec2InstanceType.MANAGER);
		}
		this.s3Handler.createBucket();
		String inputLocation = this.s3Handler.uploadFile(inputVars[0], "Tweets");
		System.out.println("Input file uploaded to:\n" + inputLocation);
		String localAppToManagersqsUrl = sqsService.createQueue(LOCAL_APP_TO_MANAGER);
		sqsService.sendMessage(inputLocation, LOCAL_APP_TO_MANAGER, localAppToManagersqsUrl);
		// S3Object file = s3Handler.downloadFile("Tweets");

	}

	private void initializeAmazonUtils() {
		System.out.println("Initializing Amazon Utils");
		this.EC2Factory = new EC2LaunchFactory(this.credentials);
		this.s3Handler = new S3Handler(credentials);
		this.sqsService = new SQSservice(credentials);
		this.ec2Client = new AmazonEC2Client(this.credentials);
		ec2Client.setEndpoint("ec2.us-east-1.amazonaws.com");

	}

	private boolean checkIfTypeExists(AmazonEC2Client ec2Client,
			Ec2InstanceType type) {
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
							System.out.println("Found active manager!!");
							return true;
						}
					}
				}
			}
		}
		return false;
	}
}
