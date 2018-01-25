package com.hive.www;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class HelloUDF extends UDF{
        public Text evaluate(Text text){
            return new Text(text.toString().toUpperCase());
        }

    public static void main(String[] args) {
       Text r= new HelloUDF().evaluate(new Text("hello world"));
        System.out.println(r);
    }
}
