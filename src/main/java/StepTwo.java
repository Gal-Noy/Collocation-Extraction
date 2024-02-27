import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepTwo {

    // TODO: add stop words
    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {

        @Override
        // <<N>decade, N> OR <w1 w2::decade, c(w1,w2)>
        public void map(Text key, LongWritable count, Context context) throws IOException,  InterruptedException {
            if (key.toString().contains("<N>")) {
                return;
            }
            String[] keyParts = key.toString().split("::");
            Text decade = new Text(keyParts[1]);
            Text bigram = new Text(keyParts[0]);
            Text w1 = new Text(bigram.toString().split(" ")[0]);

            context.write(new Text(w1 + ";;" + decade), count); // <w1;1;decade, count>
            context.write(key, count); // <w1 w2::decade, c(w1,w2)>
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static long w1Count = 0;

        @Override
        // <w1;1;decade, {c1...cn}> OR <w1 w2::decade, c(w1,w2)>
        public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException,  InterruptedException {
            if (key.toString().contains(";1;")) {
                w1Count = 0;
                for (LongWritable count : counts) {
                    w1Count += count.get();
                }
            }
            else {
                context.write(new Text(key + ";1;"), new LongWritable(w1Count)); // <w1 w2::decade;1;, c(w1)>
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        // <w1;1;decade, {c1...cn}> OR <w1 w2::decade, c(w1,w2)>
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            if (key.toString().contains(";1;")) {
                return key.toString().split(";;")[1].hashCode() % numPartitions;
            }
            return key.toString().split("::")[1].hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step Two");
        job.setJarByClass(StepTwo.class);
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

        FileInputFormat.addInputPath(job, new Path("s3://collocation-extraction-bucket/outputs/cw1w2_N/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("s3://collocation-extraction-bucket/outputs/cw1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
