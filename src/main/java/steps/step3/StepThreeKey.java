package steps.step3;

import utils.StepKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StepThreeKey extends StepKey implements WritableComparable<StepKey> {

        private final DoubleWritable npmi; // In order to sort by NPMI

        public StepThreeKey() {
            super();
            this.npmi = new DoubleWritable();
        }

        public StepThreeKey(IntWritable decade, Text w1, Text w2, Text type, DoubleWritable npmi) {
            super(decade, w1, w2, type);
            this.npmi = npmi;
        }

        public DoubleWritable getNpmi() {
            return this.npmi;
        }

        @Override
        public int compareTo(StepKey o) {
            int oDecade = o.getDecade().get();
            double mNpmi = npmi.get();
            double oNpmi = ((StepThreeKey) o).getNpmi().get();
            String oType = o.getType().toString();
            String mType = type.toString();

            // Different decades => compare decades
            if (decade.get() != oDecade) {
                return Integer.compare(decade.get(), oDecade);
            }

            // Same decades:

            // Same type keys:
            if (mType.equals(oType)) {
                if (mType.equals("W1W2")) {
                    if (mNpmi > oNpmi) {
                        return -1;
                    }
                    if (mNpmi < oNpmi) {
                        return 1;
                    }
                    return -1;
                }
                return 0;
            }

            // Different types:
            if (mType.equals("NPMI")) { // oType = W1W2
                return -1;
            }
            if (mType.equals("W1W2")) { // oType = NPMI
                return 1;
            }

            return 0;
        }

        @Override
        public String toString() {
            return decade.toString() + " " + w1.toString() + " " + w2.toString();
        }
}
