package steps.step1;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import utils.StepValue;
import utils.StopWords;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StepOne {
    private static final Text STAR = new Text("*");
    private static final long ZERO = 0;

    public static class MapperClass extends Mapper<LongWritable, Text, StepOneKey, LongWritable> {
        private static StopWords stopWords;

        public void setup(Context context) {
            String stopWordsPath = context.getConfiguration().get("stopWordsPath");
            stopWords = new StopWords(stopWordsPath);
        }

        @Override
        // input: <id, "w1 w2 \t year \t c(w1,w2)">
        // outputs:
        // <{decade, w1, w2, W1W2}, c(w1,w2)>
        // <{decade, w1, *, W1}, c(w1,w2)>
        // <{decade, *, w2, W2}, c(w1,w2)>
        // <{decade, *, *, DECADE}, c(w1,w2)>
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            try {
                String[] lineParts = line.toString().split("\t");

                String[] bigram = lineParts[0].split(" ");
                Text w1 = new Text(bigram[0].toLowerCase());
                Text w2 = new Text(bigram[1].toLowerCase());

            if (stopWords.containsWord(w1.toString()) || stopWords.containsWord(w2.toString())) {
                return;
            }

//            // Half corpus
//            if (Math.random() > 0.5) {
//                return;
//            }

                int year = Integer.parseInt(lineParts[1]);
                IntWritable decade = new IntWritable(year - year % 10);
                LongWritable cW1W2 = new LongWritable(Long.parseLong(lineParts[2]));

                context.write(new StepOneKey(decade, w1, w2, new Text("W1W2")), cW1W2);
                context.write(new StepOneKey(decade, w1, STAR, new Text("W1")), cW1W2);
                context.write(new StepOneKey(decade, STAR, w2, new Text("W2")), cW1W2);
                context.write(new StepOneKey(decade, STAR, STAR, new Text("N")), cW1W2);
            } catch (Exception e) {
                System.out.println("[ERROR] " + e.getMessage());
            }
        }
    }

        public static class CombinerClass extends Reducer<StepOneKey, LongWritable, StepOneKey, LongWritable> {
            @Override
            // <{decade, w1, w2, W1W2}, [c(w1,w2)]> => <{decade, w1, w2, W1W2}, [c(w1,w2)]>
            // <{decade, w1, *, W1}, [c1...cn]> => <{decade, w1, *, W1}, [c(w1)]>
            // <{decade, *, w2, W2}, [c1...cm]> => <{decade, *, w2, W2}, [c(w2)]>
            // <{decade, *, *, N}, [c1...cnm]> => <{decade, *, *, N}, [N]>
            public void reduce(StepOneKey key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
                long totalCount = 0;
                for (LongWritable count : counts) {
                    totalCount += count.get();
                }
                context.write(key, new LongWritable(totalCount));
            }
        }

        public static class ReducerClass extends Reducer<StepOneKey, LongWritable, StepOneKey, StepValue> {
            private static long N = 0;
            private static long cW1 = 0;

            @Override
            // inputs:
            // <{decade, w1, w2, W1W2}, [c(w1,w2)]>
            // <{decade, w1, *, W1}, [c(w1)]>
            // <{decade, *, w2, W2}, [c(w2)]>
            // <{decade, *, *, DECADE}, [N]>
            // outputs:
            // <{decade, w1, w2, W1W2}, {c(w1,w2), c(w1), 0, N}>
            // <{decade, w1, w2, W2}, {0, 0, c(w2), N}>
            public void reduce(StepOneKey key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
                long totalCount = 0;
                for (LongWritable count : counts) {
                    totalCount += count.get();
                }

                switch (key.getType().toString()) {
                    case "N":
                        N = totalCount;
                        break;
                    case "W1":
                        cW1 = totalCount;
                        break;
                    case "W2":
                        context.write(key, new StepValue(new LongWritable(ZERO),
                                new LongWritable(ZERO),
                                new LongWritable(totalCount), // c(w2)
                                new LongWritable(N)));
                        break;
                    default:  // W1W2
                        context.write(key, new StepValue(new LongWritable(totalCount), // c(w1w2)
                                new LongWritable(cW1),
                                new LongWritable(ZERO),
                                new LongWritable(N)));
                        break;
                }
            }
        }

        public static class PartitionerClass extends Partitioner<StepOneKey, LongWritable> {
            // Partition by decade
            @Override
            public int getPartition(StepOneKey key, LongWritable value, int numPartitions) {
                return (key.getDecade().get() % 100 / 10) % numPartitions;
            }
        }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        if (args.length != 4) {
            System.err.println("Usage: StepOne <stopWordsPath> <inputPath> <outputPath>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("stopWordsPath", args[1]);

        Job job = Job.getInstance(conf, "Step One");
        job.setJarByClass(StepOne.class);

        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(StepOneKey.class);
        job.setMapOutputValueClass(LongWritable.class); // Counts for w1w2, w1, w2 or decade

        job.setOutputKeyClass(StepOneKey.class);
        job.setOutputValueClass(StepValue.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("[DEBUG] STEP 1 finished!");
    }

}
