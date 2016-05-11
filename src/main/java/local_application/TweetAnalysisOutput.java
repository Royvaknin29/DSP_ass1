package local_application;

import java.util.List;

public class TweetAnalysisOutput {

	private String tweet;
	private int score;
	private List<String> sentiments;
	
	public TweetAnalysisOutput(String tweet, int score, List<String> sentiments) {
		this.tweet = tweet;
		this.score = score;
		this.sentiments = sentiments;
	}
	public TweetAnalysisOutput(){
	}
	
	
	public String getTweet() {
		return tweet;
	}

	public int getScore() {
		return score;
	}

	public List<String> getSentiments() {
		return sentiments;
	}

}
