package local_application;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class RunLocalApplication {
	private static String accKey = "";
	private static String secKey = "";

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
