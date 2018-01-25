package www.huawei.com;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;

public class CustomWritable implements WritableComparable<CustomWritable> {
    private String first;
    private int second;

    public void setFirst(String first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public CustomWritable() {
    }

    public CustomWritable(String first, int second) {
        this.first = first;
        this.second = second;
    }

    /**
     * 第一个字段相同再比较第二个字段
     * @param o
     * @return
     */
    public int compareTo(CustomWritable o) {
        int comp=this.first.compareTo(o.first);
        if(0!=comp){
            return comp;
        }

        return Integer.valueOf(second).compareTo(o.getSecond());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    public void readFields(DataInput dataInput) throws IOException {
       first= dataInput.readUTF();
       second =dataInput.readInt();
    }

    @Override
    public String toString() {
        return "CustomWritable{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomWritable that = (CustomWritable) o;

        if (second != that.second) return false;
        return first != null ? first.equals(that.first) : that.first == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + second;
        return result;
    }
}
