import twitter4j.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TweetAggregator {
    private final static String OUTPUT = "src/data/input/tweets-large.json";

    private static TwitterStream twitterStream;

    /*
     *
     */
    public static void main(String[] args) throws TwitterException, IOException {
        FileWriter fw = new FileWriter(OUTPUT, true);
        BufferedWriter bw = new BufferedWriter(fw);

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(new StatusListener() {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + 86400000;
            long timeRemaining;
            int tweetCount = 0;
            @Override
            public void onStatus(Status status) {
                if (System.currentTimeMillis() < endTime) {

                    PrintWriter out = new PrintWriter(bw);
                    out.println(TwitterObjectFactory.getRawJSON(status));
                    tweetCount++;

                    timeRemaining = (endTime - System.currentTimeMillis());
                    if (timeRemaining % 3600000 == 0) {
                        System.out.print("Hours remaining: " + timeRemaining / 3600000);
                        System.out.println("Tweets captured: " + tweetCount);
                    }

//                  Wait half a second between captures to reduce size of file while maintaining even distribution
                    long startWait = System.currentTimeMillis();
                    while (System.currentTimeMillis() < startWait + 499) {}
                }
                else { // Collection finished, close stream.
                    try { bw.flush(); }
                    catch (IOException e) { e.printStackTrace(); }
                    System.out.println(tweetCount + " total tweets saved. ");
                    TweetAggregator.stop();
                }
            }
            /* Unimplemented Interface Methods */
            @Override
            public void onException(Exception ex)
            {
                ex.printStackTrace();
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            @Override
            public void onScrubGeo(long userId, long upToStatusId) {}
            @Override
            public void onStallWarning(StallWarning warning) {}
        });
        // Initiate stream, filtering for tweets in English only
        twitterStream.sample("en");
        System.out.println("Starting at time: " + System.currentTimeMillis());
        System.out.println("Hours remaining: 24");
    }

    private static void stop() {
        twitterStream.clearListeners();
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }
}