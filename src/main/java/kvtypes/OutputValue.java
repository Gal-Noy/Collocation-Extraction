package kvtypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutputValue implements WritableComparable<OutputValue> {
    private final LongWritable cW1W2;
    private final LongWritable cW1;
    private final LongWritable cW2;
    private final LongWritable cDecade; // N

    public OutputValue() {
        cW1W2 = new LongWritable();
        cW1 = new LongWritable();
        cW2 = new LongWritable();
        cDecade = new LongWritable();
    }

    public OutputValue(LongWritable cW1W1, LongWritable cW1, LongWritable cW2, LongWritable cDecade) {
        this.cW1W2 = cW1W1;
        this.cW1 = cW1;
        this.cW2 = cW2;
        this.cDecade = cDecade;
    }

    public LongWritable getCW1W2() {
        return cW1W2;
    }

    public LongWritable getCW1() {
        return cW1;
    }

    public LongWritable getCW2() {
        return cW2;
    }

    public LongWritable getCDecade() {
        return cDecade;
    }

    @Override
    public int compareTo(OutputValue o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        cW1W2.write(dataOutput);
        cW1.write(dataOutput);
        cW2.write(dataOutput);
        cDecade.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        cW1W2.readFields(dataInput);
        cW1.readFields(dataInput);
        cW2.readFields(dataInput);
        cDecade.readFields(dataInput);
    }

    @Override
    public String toString() {
        return cW1W2.toString() + "\t" + cW1.toString() + "\t" + cW2.toString() + "\t" + cDecade.toString();
    }
}
