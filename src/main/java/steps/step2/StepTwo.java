package steps.step2;

import utils.StepValue;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StepTwo {

    public static class MapperClass extends Mapper<LongWritable, Text, StepTwoKey, StepValue> {
        @Override
        // <id, "decade w1 w2 W1W2 \t c(w1,w2) c(w1) 0 N"> => <{decade, w1, w2, W1W2}, {c(w1,w2), c(w1), 0, N}>
        // <id, "decade * w2 W2 \t 0 0 c(w2) N"> => <{decade, *, w2, W2}, {0, 0, c(w2), N}>
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineParts = value.toString().split("\t");

            String[] keyParts = lineParts[0].split(" ");
            String decade = keyParts[0], w1 = keyParts[1], w2 = keyParts[2], type = keyParts[3];

            String[] valueParts = lineParts[1].split(" ");
            long cW1W2 = Long.parseLong(valueParts[0]), cW1 = Long.parseLong(valueParts[1]), cW2 = Long.parseLong(valueParts[2]), N = Long.parseLong(valueParts[3]);

            StepTwoKey newKey = new StepTwoKey(new IntWritable(Integer.parseInt(decade)), new Text(w1), new Text(w2), new Text(type));
            StepValue newValue = new StepValue(new LongWritable(cW1W2), new LongWritable(cW1), new LongWritable(cW2), new LongWritable(N));

            context.write(newKey, newValue);
        }
    }

    public static class ReducerClass extends Reducer<StepTwoKey, StepValue, StepTwoKey, DoubleWritable> {
        private static long cW2 = 0;
        private static final Text STAR = new Text("*");

        @Override
        // inputs:
        // <{decade, w1, w2, W1W2}, {c(w1,w2), c(w1), 0, N}>
        // <{decade, *, w2, W2}, {0, 0, c(w2), N}>
        // outputs:
        // <{decade, w1, w2, W1W2}, npmi>
        // <{decade, *, *, NPMI}, npmi>
        public void reduce(StepTwoKey key, Iterable<StepValue> counts, Context context) throws IOException, InterruptedException {
            StepValue value = counts.iterator().next();

            switch (key.getType().toString()) {
                case "W2":
                    cW2 = value.getCW2().get();
                    break;
                case "W1W2":
                    long cW1W2 = value.getCW1W2().get();
                    long cW1 = value.getCW1().get();
                    long N = value.getCDecade().get();
                    double pmi = Math.log(cW1W2) + Math.log(N) - Math.log(cW1) - Math.log(cW2);
                    double pW1W2 = (double) cW1W2 / N;
                    pW1W2 = pW1W2 == 0.0 ? 0.01 : pW1W2 == 1.0 ? 0.99 : pW1W2;
                    double npmi = -pmi / Math.log(pW1W2);

                    // For extracting minPmi
                    context.write(key, new DoubleWritable(npmi));
                    // For extracting relMinPmi
                    context.write(new StepTwoKey(key.getDecade(), STAR, STAR, new Text("NPMI")), new DoubleWritable(npmi));

                    break;
            }
        }
    }

    public static class PartitionerClass extends Partitioner<StepTwoKey, StepValue> {
        // Partition by decade
        @Override
        public int getPartition(StepTwoKey key, StepValue value, int numPartitions) {
            return (key.getDecade().get() % 100 / 10) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        if (args.length != 3) {
            System.err.println("Usage: StepTwo <inputPath> <outputPath>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step Two");
        job.setJarByClass(StepTwo.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(StepTwoKey.class);
        job.setMapOutputValueClass(StepValue.class);

        job.setOutputKeyClass(StepTwoKey.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("[DEBUG] STEP 2 finished!");
    }
}
