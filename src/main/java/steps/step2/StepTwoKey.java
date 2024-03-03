package steps.step2;

import kvtypes.StepKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StepTwoKey extends StepKey implements WritableComparable<StepKey>{

    public StepTwoKey() {
        super();
    }

    public StepTwoKey(IntWritable decade, Text w1, Text w2, Text type) {
        super(decade, w1, w2, type);
    }

    @Override
    public int compareTo(StepKey o) {
        int oDecade = o.getDecade().get();
        Text oW1 = o.getW1();
        Text oW2 = o.getW2();
        String oType = o.getType().toString();
        String mType = type.toString();

        // Different decades => compare decades
        if (decade.get() != oDecade) {
            return Integer.compare(decade.get(), oDecade);
        }

        // Same decades:

        // Same type keys:
        if (mType.equals(oType)) {
            return w2.compareTo(oW2);
        }

        // Different types:
        if (mType.equals("W2")) { // oType = W1W2
            if (w2.equals(oW2))
                return -1;
            return w2.compareTo(oW2);
        }
        if (mType.equals("W1W2")) { // oType = W2
            if (w2.equals(oW2))
                return 1;
            return w2.compareTo(oW2);
        }
        return 0;
    }
}