package steps.step3;

import utils.StepKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
            DoubleWritable oNpmi = ((StepThreeKey) o).getNpmi();
            String oType = o.getType().toString();
            String mType = type.toString();

            // Different decades => compare decades
            if (decade.get() != oDecade) {
                return Integer.compare(decade.get(), oDecade);
            }

            // Same decades:

            // Same type keys:
            if (mType.equals(oType)) {
                if (mType.equals("NPMI")) {
                    return 0;
                }
                int npmiCompare = npmi.compareTo(oNpmi);
                return npmiCompare == 0 ? 1 : -npmiCompare; // In order to sort in descending order
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
        public void write(DataOutput dataOutput) throws IOException {
            super.write(dataOutput);
            npmi.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            super.readFields(dataInput);
            npmi.readFields(dataInput);
        }

        @Override
        public String toString() {
            return decade.toString() + " " + w1.toString() + " " + w2.toString();
        }
}
