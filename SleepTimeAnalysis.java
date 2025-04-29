import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SleepTimeAnalysis {

    public static class SleepMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text hour = new Text();

        private boolean lookingForT = true;
        private String currentHour = null;
        private boolean containsSleep = false;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                if (currentHour != null && containsSleep) {
                    hour.set(currentHour);
                    context.write(hour, one);
                }
                // Reset for next tweet
                lookingForT = true;
                currentHour = null;
                containsSleep = false;
            } else {
                String[] tokens = line.split("\\s+");
                if (lookingForT && tokens.length >= 3 && tokens[0].equals("T")) {
                    String time = tokens[2]; 
                    String[] timeParts = time.split(":");
                    if (timeParts.length >= 1) {
                        currentHour = timeParts[0];
                    }
                    lookingForT = false;
                } else if (tokens.length >= 1 && tokens[0].equals("W")) {
                    // Check if content contains "sleep" 
                    String content = line.substring(2).toLowerCase();
                    if (content.contains("sleep")) {
                        containsSleep = true;
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sleep Tweet Time Count");
        job.setJarByClass(SleepTimeAnalysis.class);
        job.setMapperClass(SleepMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
