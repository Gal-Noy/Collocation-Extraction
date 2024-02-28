package kvtypes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StepOneKey implements WritableComparable<StepOneKey>{
    private final IntWritable decade;
    private final Text w1;
    private final Text w2;
    private final Text type;

    public StepOneKey() {
        decade = new IntWritable();
        w1 = new Text();
        w2 = new Text();
        type = new Text();
    }

    public StepOneKey(IntWritable decade, Text w1, Text w2, Text type) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = w2;
        this.type = type;
    }

    public IntWritable getDecade() {
        return decade;
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public Text getType() {
        return type;
    }

    @Override
    public int compareTo(StepOneKey o) {
        int oDecade = o.getDecade().get();
        Text oW1 = o.getW1();
        Text oW2 = o.getW2();
        String oType = o.getType().toString();
        String mType = type.toString();

        // Both are decade keys or different decades => compare decades
        if (mType.equals("N") && oType.equals("N") ||
                decade.get() != oDecade) {
            return Integer.compare(decade.get(), oDecade);
        }

        // Same decades:

        // Same type (non-decade) keys => compare lexicographically
        if (mType.equals(oType)) {
            switch (mType) {
                case "W1":
                    return w1.compareTo(oW1);
                case "W2":
                    return w2.compareTo(oW2);
                case "W1W2":
                    if (w1.equals(oW1))
                        return w2.compareTo(oW2);
                    return w1.compareTo(oW1);
            }
        }

        // Different types:

        // If one of the keys is a decade key, it is smaller
        if (mType.equals("N"))
            return -1;
        if (oType.equals("N"))
            return 1;

        if (mType.equals("W1")) {
            switch (oType) {
                case "W2":
                    return -1;
                case "W1W2":
                    if (w1.equals(oW1))
                        return -1;
                    return w1.compareTo(oW1);
            }
        }
        if (oType.equals("W1")) {
            switch (mType) {
                case "W2":
                    return 1;
                case "W1W2":
                    if (w1.equals(oW1))
                        return 1;
                    return w1.compareTo(oW1);
            }
        }
        if (mType.equals("W1W2"))
            return -1;
        return 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        decade.write(dataOutput);
        w1.write(dataOutput);
        w2.write(dataOutput);
        type.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade.readFields(dataInput);
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        type.readFields(dataInput);
    }

    @Override
    public String toString() {
        return decade.toString() + "\t" + w1.toString() + "\t" + w2.toString() + "\t" + type.toString();
    }
}
