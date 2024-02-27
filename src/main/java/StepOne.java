import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepOne {

    // TODO: add stop words
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        // <id, w1 w2 \t year \t c(w1,w2)>
        public void map(LongWritable key, Text line, Context context) throws IOException,  InterruptedException {
            String[] lineParts = line.toString().split("\t");
            Text bigram = new Text(lineParts[0]);
            Text w1 = new Text(bigram.toString().split(" ")[0]);
            Text w2 = new Text(bigram.toString().split(" ")[1]);
            int year = Integer.parseInt(lineParts[1]);
            Text decade = new Text(String.valueOf(year - year % 10));
            long count = Long.parseLong(lineParts[2]);

            context.write(new Text(decade + ":1:" + w1), new LongWritable(count)); // <decade:1:w1, count>
            context.write(new Text(decade + ":2:" + w2), new LongWritable(count)); // <decade:2:w2, count>
            context.write(new Text(decade + ":3:" + bigram), new LongWritable(count)); // <decade:3:w1 w2, c(w1,w2)>
            context.write(decade, new LongWritable(count)); // <decade, c(w1,w2)>
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static long w1Count = 0;
        private static long w2Count = 0;
        @Override
        // <decade:1:w1, {c1...cn}> OR <decade:2:w2, {c1...cn}> OR <decade:3:w1 w2, {c(w1,w2)}> OR <decade, {c1...cn}>
        public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException,  InterruptedException {
            long totalCount = 0;
            for (LongWritable count : counts) {
                totalCount += count.get();
            }
            if (key.toString().contains(":1:")) {
                w1Count = totalCount;
            }
            else if (key.toString().contains(":2:")) {
                w2Count = totalCount;
            }
            else if (key.toString().contains(":3:")) {
                context.write(key, new LongWritable(totalCount)); // <decade:3:w1 w2, c(w1,w2)>
                context.write(new Text(key + ":1:"), new LongWritable(w1Count)); // <decade:3:w1 w2:1:, c(w1)>
                context.write(new Text(key + ":2:"), new LongWritable(w2Count)); // <decade:3:w1 w2:2:, c(w2)>
            }
            else {
                context.write(new Text(key + "<N>"), new LongWritable(totalCount)); // <decade<N>, N>
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        //
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            if (key.toString().contains(":")) {
                return key.toString().split(":")[0].hashCode() % numPartitions;
            }
            else {
                return key.toString().hashCode() % numPartitions;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step One");
        job.setJarByClass(StepOne.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://collocation-extraction-bucket/inputs/bigrams-test.txt"));

//        FileInputFormat.addInputPath(job, new Path("s3://collocation-extraction-bucket/inputs/bigrams-test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://collocation-extraction-bucket/outputs/cw1w2_N"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
