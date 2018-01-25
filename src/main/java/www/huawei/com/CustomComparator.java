package www.huawei.com;

import org.apache.hadoop.io.RawComparator;

public class CustomComparator implements RawComparator<CustomWritable> {
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        return 0;
    }

    public int compare(CustomWritable o1, CustomWritable o2) {
        return 0;
    }
}
