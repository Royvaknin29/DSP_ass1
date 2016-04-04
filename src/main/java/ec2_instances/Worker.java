package ec2_instances;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import ass1.amazon_utils.SQSservice;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.google.common.collect.Lists;

public class Worker {
	private static String accKey = "";
	private static String secKey = "";
    private static String jobsQueue = "jobsQueue";
    private static String resultsQueue = "resultsQueue";
	
    public static void main(String[] args) {
    	
    	AWSCredentials credentials = setCredentialsFromArgs(accKey, secKey);
    	SQSservice mySqsService = new SQSservice(credentials);
    	AmazonSQSClient sqsClient = new AmazonSQSClient(credentials);
		String managerToWorkerUrl = sqsClient.getQueueUrl(jobsQueue).getQueueUrl();
		String workerToManagerUrl = sqsClient.getQueueUrl(resultsQueue).getQueueUrl();
		List<String> jobsFromQueue  = getJobsFromQueue(mySqsService, managerToWorkerUrl);
        List<String> resultAfterAnalysis = preformTweetAnalysis(jobsFromQueue);
        addMessagesToQueue(resultAfterAnalysis, mySqsService, workerToManagerUrl);

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

    public static List<String> getJobsFromQueue(SQSservice mySqsService, String queueUrl) {
    	List<String> messagesContents = Lists.newArrayList();
    	//according to documentation recieveMessages should return 1-10 messages. 
    	//we need to check if we should limit it or keep allof them.
    	List<Message> messages = mySqsService.recieveMessages(jobsQueue, queueUrl);
    	for(Message jobMessage: messages){
    		messagesContents.add(jobMessage.getBody());
    		System.out.println("job Id" + jobMessage.getMessageId() + " Link:\n"+jobMessage.getBody() + "\nTaken from queue");
    	}
    	//Deleteing from Queue:
    	for(Message jobMessageToDelete: messages){
    		mySqsService.deleteMessage(jobMessageToDelete, queueUrl);
    		System.out.println("job Id" + jobMessageToDelete.getMessageId() + "\nDeleted!");
    	}
    	return messagesContents;
    }

    public static void addMessagesToQueue(List<String> messagesToAdd, SQSservice sqsService, String resultsQueueUrl) {
        for (String message: messagesToAdd) {
            sqsService.sendMessage(message, resultsQueue, resultsQueueUrl);
        }
    }

    public static List<String> preformTweetAnalysis(List<String> tweetLinks) {
    	List<String> analysedTweets = Lists.newArrayList();
        try {
            for(String tweetLink: tweetLinks){
        	Document doc = Jsoup.connect(tweetLink).get();
            String tweet = doc.select("title").text();
            findSentiment(tweet);
            analysedTweets.add(printEntities(tweet));
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        return analysedTweets;
    }

    /*** Tweet Analysis ***/

    public static String printEntities(String tweet) {

        String analyzedTweet = "";
        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        StanfordCoreNLP NERPipeline = new StanfordCoreNLP(props);

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                System.out.println("\t-" + word + ":" + ne);
                analyzedTweet += "\t-" + word + ":" + ne + "\n";
            }
        }
        return analyzedTweet;
    }

    public static int findSentiment (String tweet){

        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP sentimentPipeline = new StanfordCoreNLP(props);

        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }
}

