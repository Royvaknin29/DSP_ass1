package local_application;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class RunLocalApplication {
	private static String accKey = "AKIAJIWZRPBTMYXF4K7Q";
	private static String secKey = "Ad6zT9adXQTs7e1b3jx09+s0fbXi/5X9qoUbX2ra";

	public static void main(String[] args) {
		AWSCredentials credentials = setCredentialsFromArgs(accKey, secKey);
		if (args.length < 3) {
			System.out.println("Missing arguments!\naborting...");
			System.exit(1);
		}
		LocalApplication localApplication = new LocalApplication(credentials, args[1]);
		localApplication.startApplication(args);
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
}
