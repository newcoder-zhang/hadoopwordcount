package com.baidu.www;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextIntWritable implements WritableComparable<TextIntWritable> {
    private String first;
    private int second;

    public TextIntWritable() {
    }

    public TextIntWritable(String  first, int second) {
        this.first = first;
        this.second = second;
    }

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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TextIntWritable)) return false;

        TextIntWritable that = (TextIntWritable) o;

        if (getSecond() != that.getSecond()) return false;
        return getFirst().equals(that.getFirst());
    }

    @Override
    public int hashCode() {
        int result = getFirst().hashCode();
        result = 31 * result + getSecond();
        return result;
    }

    public int compareTo(TextIntWritable o) {
        if(!first.equals(o.first)){
            return Integer.valueOf(first)>Integer.valueOf(o.first)?1:-1;
        }else if(second!=o.second){
            return second>o.second?1:-1;
        }
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);

    }

    public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readUTF();
            second=dataInput.readInt();
    }
}
