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
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


public class Analyzer {
    private static final String OUTPUT = "src/data/output/";

    private static int uniqueCount = 0;
    private static int duplicateCount = 0;

    private static BufferedWriter dupFileOut;
    private static BufferedWriter uniqFileOut;

    List<Date> times = new ArrayList<>();

    public static void printUniquenessRatio() throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TwitterAnalysis");
        job.setJarByClass(Analyzer.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

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

        System.out.print(uniqueCount + " Unique Words, " + duplicateCount + " Duplicate Words, ");
        int ratio = duplicateCount > 0 ? Math.round(uniqueCount / duplicateCount) : 0;
        if (ratio > 0)
            System.out.println(ratio + ":1" + " ratio ");
    }

    public static void getBestPostTimes() {}

    public static class MapBestTimes extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) {
        }
    }

    public static class ReduceBestTimes extends Reducer<Text, IntWritable, NullWritable, NullWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
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

    public static class Reduce extends Reducer<Text, IntWritable, NullWritable, NullWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();

            PrintWriter out;
            if (sum > 1) {
                duplicateCount++;
                out = new PrintWriter(dupFileOut, true);
            }
            else {
                uniqueCount++;
                out = new PrintWriter(uniqFileOut, true);
            }
            out.println(key.toString() + " : " + sum);
        }
    }

}
