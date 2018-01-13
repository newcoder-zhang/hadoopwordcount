package www.huawei.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
        String uri="hdfs://beifeng:8020";
        Configuration config=new Configuration();
        FileSystem fs= FileSystem.get(URI.create(uri),config);
        FileStatus[] statuses=fs.listStatus(new Path("input"));
        for (FileStatus file:statuses) {
            System.out.println(file);
        }
    }
}
