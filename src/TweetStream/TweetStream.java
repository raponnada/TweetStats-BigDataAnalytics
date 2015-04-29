package com.umkc.twitterstream;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.FilterQuery;
import twitter4j.TwitterStreamFactory;
import twitter4j.StallWarning;
import twitter4j.StatusDeletionNotice;
import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;


import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TweetStream {
		public static int count;
	public static void main(String[] args) {
		
		
		
		//Sending OAuth authorized requests to twitter for connecting to Streaming API's
		ConfigurationBuilder configBuild = new ConfigurationBuilder();
		configBuild.setDebugEnabled(true);
		configBuild.setJSONStoreEnabled(true);
		configBuild.setOAuthConsumerKey("CFlVrl5AYdJ24Ql9XwGcIwAh2");
		configBuild.setOAuthConsumerSecret("6nNIz5gm1r5gM0XpDV76YRg7n6ZXr7lMlcFsMpoQDvvM5kX7vu");
		configBuild.setOAuthAccessToken("144477388-Rt3qf2gLq76iANk7YLykOKJq6UwzcF1JJaFNbCUF");
		configBuild.setOAuthAccessTokenSecret("37QP0K0oXGLHUqI5KnPm9Pfp6nekZNntzFX5CerVE1kVx");
		

		TwitterStream twitterStream = new TwitterStreamFactory(configBuild.build()).getInstance();

		StatusListener tweetListener = new StatusListener() {

			// Getting the TweetStream and storing in JSON file using File Writer
			@Override
			public void onStatus( Status tweetStatus) {
				
				String rawTweets = TwitterObjectFactory.getRawJSON(tweetStatus);
				FileWriter tweetFile = null;
				
			    try {
			        tweetFile = new FileWriter("/home/spykid/workspace/BigData/Tweets.txt", true);
					PrintWriter tweetWriter = new PrintWriter(tweetFile);
					if(count == 0)
						rawTweets = '['+rawTweets;
					rawTweets = rawTweets+',';
					if(count == 2999)
						rawTweets = rawTweets + ']';
					;
					System.out.println(rawTweets);
					System.out.println(count);
					tweetWriter.println(rawTweets);
											
					tweetFile.flush();
					tweetFile.close();
					if(count == 3000)
						System.exit(0);
					
					count++;
					
				} catch (IOException e) {
					e.printStackTrace();
				  }
			    
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
			@Override
			public void onStallWarning(StallWarning warning) {}
			@Override
			public void onException(Exception exception) {}
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {}

		};
		
		double[][] tweetLoc = { { -180, -90 }, { 180, 90 } };
		String[] tweetLang = { "en" };
		
		// Filtering Tweets based on Location and Language
		FilterQuery tweetFilter = new FilterQuery();
		tweetFilter.language(tweetLang);
		tweetFilter.locations(tweetLoc);
		
		// Adding Status Listener to Twitter Stream
		twitterStream.addListener(tweetListener);
		
		// Filtering Twitter stream data based on the Filter Query
		twitterStream.filter(tweetFilter);
		
	}
}