package ass1_.amazon_utils;

import java.util.List;

import static org.apache.commons.codec.binary.Base64.encodeBase64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.ShutdownBehavior;

public class EC2Launch {
	private static String KEY_NAME = "dsp162";
	private static String SECURITY_GROUP = "launch-wizard-1";
	private static String INSTANCE_TYPE = "t2.micro";

	public static void main(String[] args) throws Exception {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (~/.aws/credentials), and is in valid format.",
					e);
		}

		AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
		amazonEC2Client.setEndpoint("ec2.us-east-1.amazonaws.com");

		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
		runInstancesRequest
				.setInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate);
		runInstancesRequest.withImageId("ami-34f4fe5e")
				.withInstanceType(INSTANCE_TYPE).withMinCount(1)
				.withMaxCount(1).withKeyName(KEY_NAME)
				.withSecurityGroups(SECURITY_GROUP)
		 .setUserData(generateShellExtractionCommand());
		RunInstancesResult runInstancesResult = amazonEC2Client
				.runInstances(runInstancesRequest);

	}

	public static String generateShellExtractionCommand() {
		return new String(encodeBase64("#!/bin/bash\ncd ~\nwget \"https://s3.amazonaws.com/ass-1-bucket/helloWorldZip.zip\"\nunzip -P `cat ./pass` helloWorldZip\njava -jar helloWorld.jar > log".getBytes()));
		 
	}
}