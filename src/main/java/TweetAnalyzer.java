import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.TreeMap;

public class TweetAnalyzer {
    private static final String OUTPUT = "src/data/output/";

    private static int uniqueCount = 0;
    private static int duplicateCount = 0;

    private static BufferedWriter dupFileOut;
    private static BufferedWriter uniqFileOut;

    private static TreeMap<Long, Integer> times = new TreeMap<>();

    public static void printUniquenessRatio() throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "UniquenessRatio");
        job.setJarByClass(TweetAnalyzer.class);

        job.setMapperClass(MapUniqueWords.class);
        job.setReducerClass(ReduceUniqueWords.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("src/data/input/tweets-small.json"));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT));

        dupFileOut = new BufferedWriter(new FileWriter(OUTPUT + "dups.txt", true));
        uniqFileOut = new BufferedWriter(new FileWriter(OUTPUT + "uniqs.txt", true));

        job.waitForCompletion(true);

        System.out.println(uniqueCount + " Unique Words : " + duplicateCount + " Duplicate Words");

        BigInteger unique = BigInteger.valueOf(uniqueCount);
        BigInteger duplicate = BigInteger.valueOf(duplicateCount);
        BigInteger gcd = unique.gcd(duplicate);
        BigInteger uniqueReduced = unique.divide(gcd);
        BigInteger duplicateReduced = duplicate.divide(gcd);
        BigDecimal ratio = BigDecimal.valueOf(uniqueReduced.doubleValue()).divide(BigDecimal.valueOf(duplicateReduced.doubleValue()), 3, RoundingMode.HALF_EVEN);

        System.out.format("Ratio - %d:" + "%d (%.3f)", uniqueReduced, duplicateReduced, ratio);
        System.out.println();
    }

    public static void getBestPostTimes() throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "BestTweetTimes");
        job.setJarByClass(TweetAnalyzer.class);

        job.setMapperClass(MapBestTimes.class);
        job.setReducerClass(ReduceBestTimes.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("src/data/input/tweets-small.json"));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT + "times/"));

        job.waitForCompletion(true);

        System.out.println();
        System.out.println("Best Times to Post Tweets: ");
        for (java.util.Map.Entry<Long, Integer> entry : times.entrySet()) {
            if (entry.getKey() < 10 || entry.getKey() > 12) { System.out.print("0"); }
            System.out.println((entry.getKey() >= 12 ? entry.getKey() - 12 + ":00 PM" : entry.getKey() + ":00 AM") + " - " + entry.getValue() + " Retweets/Favorites");
        }
        System.out.println();
    }

    public static class MapUniqueWords extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) {
            word.clear();
            String line = value.toString().trim();

            try {
                Status status = TwitterObjectFactory.createStatus(line);
                Arrays.stream(status.getText().split("[^\\w]")).forEach(w -> {
                    word.set(w.toLowerCase());
                    try {
                        context.write(word, one);
                    }
                    catch (IOException | InterruptedException e) { e.printStackTrace(); }
                });
            }
            catch (TwitterException tE) { tE.printStackTrace(); }
        }
    }

    public static class ReduceUniqueWords extends Reducer<Text, IntWritable, NullWritable, NullWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();

            PrintWriter out;
            if (sum == 1) {
                uniqueCount++;
                out = new PrintWriter(uniqFileOut, true);
            }
            else {
                duplicateCount++;
                out = new PrintWriter(dupFileOut, true);
            }
            out.println(key.toString() + " : " + sum);
        }
    }

    public static class MapBestTimes extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context context) {
            String line = value.toString().trim();

            try {
                Status status = TwitterObjectFactory.createStatus(line);
                if (status.getRetweetedStatus() != null)
                    status = status.getRetweetedStatus();
                int overallCount = status.getFavoriteCount() + status.getRetweetCount();
                long timeCreated = status.getCreatedAt().getTime();
                Long hourCreated = Long.parseLong(new SimpleDateFormat("HH").format(new Date(timeCreated)));
                context.write(new LongWritable(hourCreated), new IntWritable(overallCount));
            }
            catch (Exception e) { e.printStackTrace(); }
        }
    }

    public static class ReduceBestTimes extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if (times.size() < 10) {
                times.put(key.get(), sum);
            }
            else {
                java.util.Map.Entry minEntry = getSmallestEntry();
                Integer minValue = (Integer) minEntry.getValue();
                if (sum > minValue) {
                    times.remove(minEntry.getKey());
                    times.put(key.get(), sum);
                }
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static java.util.Map.Entry getSmallestEntry() {
        if (times.isEmpty())
            return null;

        Iterator iterator = times.entrySet().iterator();
        if (iterator == null)
            return null;

        java.util.Map.Entry smallestEntry = (java.util.Map.Entry) iterator.next();
        while (iterator.hasNext()) {
            Integer smallestValue = (Integer) smallestEntry.getValue();
            java.util.Map.Entry entryToCheck = (java.util.Map.Entry) iterator.next();
            Integer valueToCheck = (Integer) entryToCheck.getValue();
            if (valueToCheck.compareTo(smallestValue) < 0)
                smallestEntry = entryToCheck;
        }

        return smallestEntry;
    }
}
