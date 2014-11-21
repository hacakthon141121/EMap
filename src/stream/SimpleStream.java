package stream;
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.RateLimitStatusEvent;
import twitter4j.RateLimitStatusListener;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import database.DatabaseOperation;

public class SimpleStream {
	static int numOfRecordsCollected = 0, limit = 100000;
	static String[] listHappiness = {":-)", ":)", ";)", "<3", ":D", ":P", ":D", ":P", ";-)", ":-D", "XD", ":')", "=)", "C:", ":L", ":3", ";D", ":-P", ";-P", ":d", ":*", "=D", "d^_^b", ":-9", ":'-)", ":)))", "n_n", ":O)", "^_^", "=P", "(;", ":]", ";P", "/-)", ":'3", "^.^", ":}", ";-D", ";')", "=]", "C':", ";]", ":-*", ":'D", ":1", "(-;", ";L", ">:)", "X-D", "B-)", ":>", "8-D"};
	
    public static void main(String[] args) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
//        cb.setOAuthConsumerKey("your consumer key");
//        cb.setOAuthConsumerSecret("your consumer secret");
//        cb.setOAuthAccessToken("your access token");
//        cb.setOAuthAccessTokenSecret("your access token secret");
        
        final DatabaseOperation db = new DatabaseOperation();
        try {
			db.connect();
		} catch (Exception e) {
			// TODO: handle exception
		}

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        StatusListener listener = new StatusListener() {
            @Override
            public void onException(Exception arg0) {
                // TODO Auto-generated method stub
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {
                // TODO Auto-generated method stub
            }

            @Override
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub
            }

            @Override
            public void onStatus(Status status) {
            	if(numOfRecordsCollected > limit){
            		System.exit(0);
            	}
            	
            	String content = status.getText();
                GeoLocation location = status.getGeoLocation();
                double xlabel = location.getLatitude();
                double ylabel = location.getLongitude();
                String emoticon = null;
                
                // Store current tweet into database
                System.out.println(content);///
                System.out.println(xlabel);///
                System.out.println(ylabel);///
                try {
                	for(int i = 0; i < listHappiness.length; i++){
                		if(content.toLowerCase().contains(listHappiness[i].toLowerCase())){
                			emoticon = listHappiness[i];
                			System.out.println("===emoticon is '" + emoticon + "'");///
                			db.addRowToTable("happiness", content, xlabel, ylabel, emoticon);
                			break;
                		}
                	}
        			numOfRecordsCollected++;
        		} catch (Exception e) {
        			// TODO: handle exception
        		}
            }

            @Override
            public void onTrackLimitationNotice(int arg0) {
                // TODO Auto-generated method stub
            }

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
			}
        };
        
        FilterQuery fq = new FilterQuery();
    
        String keywords[] = listHappiness;

        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq); 
        twitterStream.addRateLimitStatusListener(new RateLimitStatusListener() {
        	@Override
			public void onRateLimitReached(RateLimitStatusEvent arg0) {
				// TODO Auto-generated method stub
				System.out.println("Limit["+arg0.getRateLimitStatus().getLimit() + "], Remaining[" +arg0.getRateLimitStatus().getRemaining()+"]");
			}

			@Override
			public void onRateLimitStatus(RateLimitStatusEvent arg0) {
				// TODO Auto-generated method stub
				System.out.println("Limit["+arg0.getRateLimitStatus().getLimit() + "], Remaining[" +arg0.getRateLimitStatus().getRemaining()+"]");
		    }
        } );
        
        try {
			db.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
    }
}