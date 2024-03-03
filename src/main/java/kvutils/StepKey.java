package kvutils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class StepKey implements WritableComparable<StepKey>{
    protected final IntWritable decade;
    protected final Text w1;
    protected final Text w2;
    protected final Text type;

    public StepKey() {
        decade = new IntWritable();
        w1 = new Text();
        w2 = new Text();
        type = new Text();
    }

    public StepKey(IntWritable decade, Text w1, Text w2, Text type) {
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
    public abstract int compareTo(StepKey o);

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
        return decade.toString() + " " + w1.toString() + " " + w2.toString() + " " + type.toString();
    }
}
