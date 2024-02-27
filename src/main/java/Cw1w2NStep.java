import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Cw1w2NStep {

    // TODO: add stop words
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException,  InterruptedException {
            String[] lineParts = line.toString().split("\t");
            Text bigram = new Text(lineParts[0]);
            int year = Integer.parseInt(lineParts[1]);
            Text decade = new Text(String.valueOf(year - year % 10));
            Text bigramDecade = new Text(bigram + "::" + decade);
            long count = Long.parseLong(lineParts[2]);
            context.write(bigramDecade, new LongWritable(count)); // <w1w2::decade, c(w1,w2)>
            context.write(decade, new LongWritable(count)); // <decade, c(w1, w2)> (for N)
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        // <w1w2::decade, {c(w1,w2)}> OR <decade, {c1...cn}> (for N)
        @Override
        public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException,  InterruptedException {
            long totalCounts = 0;
            for (LongWritable count : counts) {
                totalCounts += count.get();
            }

            if (key.toString().contains("::")) { // <w1w2::decade, {c(w1,w2)}>
                context.write(key, new LongWritable(totalCounts));
            }
            else { // <decade, {c1...cn}>
                context.write(new Text("<N>" + key), new LongWritable(totalCounts));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step Counts");
        job.setJarByClass(Cw1w2NStep.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://collocation-extraction-bucket/inputs/bigrams-test.txt"));

//        FileInputFormat.addInputPath(job, new Path("s3://collocation-extraction-bucket/inputs/bigrams-test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://collocation-extraction-bucket/outputs/cw1w2_N"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
