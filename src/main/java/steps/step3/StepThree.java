package steps.step3;

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
import java.util.Arrays;


public class StepThree {
    public static class MapperClass extends Mapper<LongWritable, Text, StepThreeKey, DoubleWritable> {

        @Override
        // <id, "decade w1 w2 W1W2 \t npmi"> => <{decade, w1, w2, W1W2, npmi}, npmi>
        // <id, "decade * * NPMI \t npmi"> => <{decade, 0, 0, NPMI, npmi}, npmi>
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineParts = value.toString().split("\t");

            String[] keyParts = lineParts[0].split(" ");
            String decade = keyParts[0], w1 = keyParts[1], w2 = keyParts[2], type = keyParts[3];

            double npmi = Double.parseDouble(lineParts[1]);

            StepThreeKey newKey = new StepThreeKey(new IntWritable(Integer.parseInt(decade)), new Text(w1), new Text(w2), new Text(type), new DoubleWritable(npmi));
            context.write(newKey, new DoubleWritable(npmi));
        }
    }

    public static class ReducerClass extends Reducer<StepThreeKey, DoubleWritable, StepThreeKey, Text> {
        private static double decadeNPMIs = 0;

        @Override
        // inputs:
        // <{decade, w1, w2, W1W2, npmi}, [npmi]>
        // <{decade, 0, 0, NPMI, npmi}, [npmi]>
        // output:
        // <{decade, w1, w2, W1W2, npmi}, "NPMI: npmi \t relMinPMI: rNpmi"> if npmi >= minPmi or rNpmi >= relMinPmi
        // printed as: "w1 w2 decade \t NPMI: npmi \t relMinPMI: rNpmi"
        public void reduce(StepThreeKey key, Iterable<DoubleWritable> counts, Context context) throws IOException, InterruptedException {
            try {
                if (key.getType().toString().equals("NPMI")) {
                    for (DoubleWritable count : counts) {
                        decadeNPMIs += count.get();
                    }
                } else { // W1W2
                    double npmi = counts.iterator().next().get();
                    double rNpmi = npmi / decadeNPMIs;
                    final double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
                    final double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));

                    if (npmi >= minPmi || rNpmi >= relMinPmi) {
                        context.write(key, new Text(String.format("NPMI: %.5f", npmi)));
                    }
                }
            }
            catch (Exception e) {
                System.out.println("[ERROR] " + e.getMessage());
            }
        }
    }

    public static class PartitionerClass extends Partitioner<StepThreeKey, DoubleWritable> {
        // Partition by decade
        @Override
        public int getPartition(StepThreeKey key, DoubleWritable value, int numPartitions) {
            return key.getDecade().hashCode() % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        if (args.length != 5) {
            System.out.println("Usage: StepThree <inputPath> <outputPath> <minPmi> <relMinPmi>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("minPmi", args[3]);
        conf.set("relMinPmi", args[4]);

        Job job = Job.getInstance(conf, "Step Three");
        job.setJarByClass(StepThree.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(StepThreeKey.class);
        job.setMapOutputValueClass(DoubleWritable.class); // npmi

        job.setOutputKeyClass(StepThreeKey.class);
        job.setOutputValueClass(Text.class); // "NPMI: npmi \t relMinPMI: rNpmi"

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("[DEBUG] STEP 3 finished!");
    }
}
