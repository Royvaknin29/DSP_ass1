package ec2_instances;
import java.io.IOException;
import java.util.*;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import local_application.TweetAnalysisOutput;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import com.google.common.collect.Lists;
import ass1.amazon_utils.SQSservice;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;


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
        while (!isQueueEmpty(sqsClient, managerToWorkerUrl)) {
            List<String> jobsFromQueue  = getJobsFromQueue(mySqsService, managerToWorkerUrl);
            List<TweetAnalysisOutput> resultAfterAnalysis = preformTweetAnalysis(jobsFromQueue);
            ObjectMapper mapper = new ObjectMapper();
            try {
                String jsonInString = mapper.writeValueAsString(resultAfterAnalysis);
                addMessagesToQueue(jsonInString, mySqsService, workerToManagerUrl);
            } catch (Exception e) {
                System.out.println("error serializing");
            }
        }
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

    public static boolean isQueueEmpty(AmazonSQSClient sqsClient, String queueUrl) {
        Set<String> attrs = new HashSet<String>();
        attrs.add("ApproximateNumberOfMessages");
//        CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(jobsQueue);
        GetQueueAttributesRequest a = new GetQueueAttributesRequest().withQueueUrl(queueUrl).withAttributeNames(attrs);
        Map<String,String> result = sqsClient.getQueueAttributes(a).getAttributes();
        return Integer.parseInt(result.get("ApproximateNumberOfMessages")) > 0;
    }

    public static void addMessagesToQueue(String messageToAdd, SQSservice sqsService, String resultsQueueUrl) {
        sqsService.sendMessage(messageToAdd, resultsQueue, resultsQueueUrl);
    }

    public static List<TweetAnalysisOutput> preformTweetAnalysis(List<String> tweetLinks) {
        List<TweetAnalysisOutput> results = Lists.newArrayList();
        try {
            for(String tweetLink: tweetLinks){
        	    Document doc = Jsoup.connect(tweetLink).get();
                String tweet = doc.select("title").text();
                int sentiment = findSentiment(tweet);
                results.add(new TweetAnalysisOutput(tweet, sentiment, printEntities(tweet)));
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        return results;
    }

    /*** Tweet Analysis ***/

    public static List<String> printEntities(String tweet) {

        List<String> entities = Lists.newArrayList();
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
//                System.out.println("\t-" + word + ":" + ne);
                if (!ne.equals("O")) {
                    entities.add(word + ":" + ne);
                }
            }
        }
        return entities;
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

