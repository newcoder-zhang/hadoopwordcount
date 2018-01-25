package com.baidu.www;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 组合的新数据类型 key值的比较,实际上就是compareTo中定义的规则
 */
public class CustomSortComparator extends WritableComparator{
    public CustomSortComparator(){
        super(TextIntWritable.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextIntWritable b1=(TextIntWritable)a;
        TextIntWritable b2=(TextIntWritable)b;
        return b1.compareTo(b2);
    }
}
