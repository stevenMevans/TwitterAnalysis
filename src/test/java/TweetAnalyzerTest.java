import org.junit.Assert;
import org.junit.Test;


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
        try {
            TweetAnalyzer.getBestPostTimes();
        }
        catch (Exception e) { e.printStackTrace(); }
    }

    @Test
    public void testRoundTimeUp()
    {
        long time = 10000423;

        long roundedUp0Seconds = TweetAnalyzer.roundedTime(time, 0);
        Assert.assertEquals(10000000, roundedUp0Seconds);

        long roundedUp1Second = TweetAnalyzer.roundedTime(time, 1);
        Assert.assertEquals(10001000, roundedUp1Second);

        long roundedDown1Second = TweetAnalyzer.roundedTime(time, -1);
        Assert.assertEquals(9999000, roundedDown1Second);

        long roundedUp5Seconds = TweetAnalyzer.roundedTime(time, 5);
        Assert.assertEquals(10005000, roundedUp5Seconds);

        long roundedDown5Seconds = TweetAnalyzer.roundedTime(time, -5);
        Assert.assertEquals(9995000, roundedDown5Seconds);

        long roundedUp1Minute = TweetAnalyzer.roundedTime(time, 60);
        Assert.assertEquals(10060000, roundedUp1Minute);

        long roundedDown1Minute = TweetAnalyzer.roundedTime(time, -60);
        Assert.assertEquals(9940000, roundedDown1Minute);

        long roundedUp1Hour = TweetAnalyzer.roundedTime(time, 3600);
        Assert.assertEquals(13600000, roundedUp1Hour);

        long roundedDown1Hour = TweetAnalyzer.roundedTime(time, -3600);
        Assert.assertEquals(6400000, roundedDown1Hour);

        long roundedDown3Hours = TweetAnalyzer.roundedTime(time, -10800);
        Assert.assertEquals(0, roundedDown3Hours);

        time = 10000523;

        roundedUp0Seconds = TweetAnalyzer.roundedTime(time, 0);
        Assert.assertEquals(10000000, roundedUp0Seconds);

        roundedUp1Second = TweetAnalyzer.roundedTime(time, 1);
        Assert.assertEquals(10001000, roundedUp1Second);

        roundedDown1Second = TweetAnalyzer.roundedTime(time, -1);
        Assert.assertEquals(9999000, roundedDown1Second);

        roundedUp5Seconds = TweetAnalyzer.roundedTime(time, 5);
        Assert.assertEquals(10005000, roundedUp5Seconds);

        roundedDown5Seconds = TweetAnalyzer.roundedTime(time, -5);
        Assert.assertEquals(9995000, roundedDown5Seconds);

        roundedUp1Minute = TweetAnalyzer.roundedTime(time, 60);
        Assert.assertEquals(10060000, roundedUp1Minute);

        roundedDown1Minute = TweetAnalyzer.roundedTime(time, -60);
        Assert.assertEquals(9940000, roundedDown1Minute);

        roundedUp1Hour = TweetAnalyzer.roundedTime(time, 3600);
        Assert.assertEquals(13600000, roundedUp1Hour);

        roundedDown1Hour = TweetAnalyzer.roundedTime(time, -3600);
        Assert.assertEquals(6400000, roundedDown1Hour);

        roundedDown3Hours = TweetAnalyzer.roundedTime(time, -10800);
        Assert.assertEquals(0, roundedDown3Hours);
    }
}
