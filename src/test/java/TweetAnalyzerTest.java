import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;


public class TweetAnalyzerTest {
    @Test
    public void testPrintUniquenessRatio() {
        try {
            TweetAnalyzer.printUniquenessRatio();
        }
        catch (Exception e) { e.printStackTrace(); }
    }

    @Test
    public void testGetBestPostTimes() {
        long startTime = System.currentTimeMillis();
        System.out.println("Starting At: " + new SimpleDateFormat("HH:mm:ss").format(new Date(startTime)));
        try {
            TweetAnalyzer.getBestPostTimes();
        }
        catch (Exception e) { e.printStackTrace(); }
        long stopTime = System.currentTimeMillis();
        System.out.println("Stopping At: " + new SimpleDateFormat("HH:mm:ss").format(new Date (stopTime)));
        long totalTime = stopTime - startTime;
        System.out.println("Total Time Taken: " + new SimpleDateFormat("ss").format(new Date (totalTime)) + " seconds");
    }
}
