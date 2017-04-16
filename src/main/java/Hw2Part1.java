/**
 * Created by wangjian36 on 17/4/15.
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

public class Hw2Part1 {

    // This is the Mapper class
    // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
    //
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\\n");
            for (String line : lines) {
                String[] tokens = line.split(" +");
                for (String token : tokens) {
                    System.out.println(token);
                }
                if (tokens.length != 3) {
                    continue;
                }
                String fromToPair = tokens[0] + " " + tokens[1];
                String valuePair = "1" + " " + tokens[2];
                word.set(fromToPair);
                context.write(word, new Text(valuePair));
            }
        }
    }

    public static class IntSumCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            double average = 0d;
            for (Text val : values) {
                String valuePair = val.toString();
                String[] pairs = valuePair.split(" ");
                Integer num = Integer.valueOf(pairs[0]);
                Double time = Double.valueOf(pairs[1]);
                sum += num;
                average += time;
            }
            average /= sum;
            context.write(key, new Text("" + sum + " " + average));
        }
    }

    // This is the Reducer class
    // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
    //
    // We want to control the output format to look at the following:
    //
    // count of word = count
    //
    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            double average = 0d;
            for (Text val : values) {
                String valuePair = val.toString();
                String[] pairs = valuePair.split(" ");
                Integer num = Integer.valueOf(pairs[0]);
                Double time = Double.valueOf(pairs[1]);
                sum += num;
                average += time;
            }
            average /= sum;
            BigDecimal bigDecimal = new BigDecimal(average);

            context.write(key, new Text("" + sum + " " + bigDecimal.setScale(3,BigDecimal.ROUND_HALF_UP)));
        }
    }
    private static class MyOutputFormat<K,V> extends TextOutputFormat<K,V> {

        public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            String keyValueSeparator = conf.get(SEPERATOR, " ");
            CompressionCodec codec = null;
            String extension = "";
            if(isCompressed) {
                Class file = getOutputCompressorClass(job, GzipCodec.class);
                codec = (CompressionCodec)ReflectionUtils.newInstance(file, conf);
                extension = codec.getDefaultExtension();
            }

            Path file1 = this.getDefaultWorkFile(job, extension);
            FileSystem fs = file1.getFileSystem(conf);
            FSDataOutputStream fileOut;
            if(!isCompressed) {
                fileOut = fs.create(file1, false);
                return new TextOutputFormat.LineRecordWriter(fileOut, keyValueSeparator);
            } else {
                fileOut = fs.create(file1, false);
                return new TextOutputFormat.LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add the input paths as given by command line
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        // add the output path as given by the command line
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

