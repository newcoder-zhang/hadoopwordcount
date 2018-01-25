package com.baidu.www;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 只比较字符串的大小
 */
public class CustomGroupingComparator extends WritableComparator {

    public CustomGroupingComparator(){
        super(TextIntWritable.class,true);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1,0,l1-4,b2,0,l2-4);
    }
}
