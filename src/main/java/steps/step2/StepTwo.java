package steps.step2;

import kvtypes.OutputValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import steps.step1.StepOneKey;

import java.io.IOException;

public class StepTwo {

    public static class MapperClass extends Mapper<StepTwoKey, OutputValue, StepTwoKey, OutputValue> {

        @Override
        // input: <id, w1 w2 \t year \t c(w1,w2)>
        // outputs:
        // <{decade, w1, w2, W1W2}, {c(w1,w2), c(w1), 0, N}>
        // <{decade, w1, w2, W2}, {0, 0, c(w2), N}>
        public void map(StepTwoKey key, OutputValue value, Context context) throws IOException,  InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClass extends Reducer<StepTwoKey,OutputValue, StepTwoKey, DoubleWritable> {
        private static long cW2 = 0;
        private static final Text STAR = new Text("*");

        @Override
        // inputs:
        // <{decade, w1, w2, W1W2}, {c(w1,w2), c(w1), 0, N}>
        // <{decade, w1, w2, W2}, {0, 0, c(w2), N}>
        // outputs:
        // <{decade, w1, w2, W1W2}, npmi>
        // <{decade, *, *, PMI}, npmi>
        public void reduce(StepTwoKey key, Iterable<OutputValue> counts, Context context) throws IOException,  InterruptedException {
            OutputValue value = counts.iterator().next();

            switch(key.getType().toString()) {
                case "W2":
                    cW2 = value.getCW2().get();
                    break;
                case "W1W2":
                    long cW1W2 = value.getCW1W2().get();
                    long cW1 = value.getCW1().get();
                    long N = value.getCDecade().get();
                    double npmi = Math.log(cW1W2) + Math.log(N) - Math.log(cW1) - Math.log(cW2);
                    context.write(key, new DoubleWritable(npmi));
                    context.write(new StepTwoKey(key.getDecade(), STAR, STAR, new Text("PMI")), new DoubleWritable(npmi));
                    break;
            }
        }
    }

    public static class PartitionerClass extends Partitioner<StepTwoKey, OutputValue> {
        // Partition by decade
        @Override
        public int getPartition(StepTwoKey key, OutputValue value, int numPartitions) {
            return (key.getDecade().get() % 100 / 10) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step Two");
        job.setJarByClass(StepTwo.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(StepTwoKey.class);
        job.setMapOutputValueClass(OutputValue.class);

        job.setOutputKeyClass(StepTwoKey.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("s3://collocation-extraction-bucket/outputs/step-one"));
        FileOutputFormat.setOutputPath(job, new Path("s3://collocation-extraction-bucket/outputs/step-two"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("[DEBUG] STEP 2 finished!");
    }
}
