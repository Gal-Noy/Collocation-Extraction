package steps.step1;

import kvutils.StepKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StepOneKey extends StepKey implements WritableComparable<StepKey>{

    public StepOneKey() {
        super();
    }

    public StepOneKey(IntWritable decade, Text w1, Text w2, Text type) {
        super(decade, w1, w2, type);
    }

    @Override
    public int compareTo(StepKey o) {
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
        return 0;
    }
}
