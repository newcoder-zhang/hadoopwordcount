package www.huawei.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {

/**
 * 每次连接HDFS的工作
 * 注意后续的操作父目录为 hdfs://beifeng:8020/user/beifeng/
 */
    public static DistributedFileSystem connectHDFS() {
        String uri = "hdfs://beifeng:8020";
        Configuration config = new Configuration();
        DistributedFileSystem dfs = new DistributedFileSystem();
        try {
            dfs.initialize(URI.create(uri), config);
        Path work = dfs.getWorkingDirectory();
        System.out.println("work dir...."+work);//hdfs://beifeng:8020/user/beifeng
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dfs;
    }

    /**
     *上传
     * @param src 本地文件绝对路径
     * @param des
     * @throws IOException
     */
    public static void upFileToHDFS(String src,String des) throws IOException {
        DistributedFileSystem dfs = connectHDFS();
        dfs.copyFromLocalFile(new Path(src),new Path(des));
    }

    /**下载
     * @param src hdfs上的路径
     * @param des  本地文件的绝对路径
     * @throws IOException
     */
    public static void downloadFromHDFS(String src,String des) throws IOException{

        DistributedFileSystem dfs = connectHDFS();
        dfs.copyToLocalFile(new Path(src),new Path(des));
    }
    /**
     * 创建文件夹
     * @param dirname 文件夹的名字
     */
    public static void mkdir(String dirname){
        DistributedFileSystem dfs = connectHDFS();
        try {
            dfs.mkdirs(new Path(dirname));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 级联删除文件和文件夹
     * @param filename
     */
    public static void delFile(String filename){
        DistributedFileSystem dfs = connectHDFS();
        try {
            dfs.delete(new Path(filename),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列出目录下的所有文件
     * @param path
     * @throws IOException
     */
    public static void listFile(String path) throws IOException {
        DistributedFileSystem dfs = connectHDFS();
        FileStatus[] statuses = dfs.listStatus(new Path(path));
        for (FileStatus file: statuses) {
            System.out.println(file);

        }
    }

    public static void main(String[] args) throws IOException {
        //listFile("input/");
        //mkdir("input/testinput");
//        upFileToHDFS("C:\\Users\\Administrator\\Pictures\\xx.jpg","input/testinput");
//        delFile("input/testinput/xx.jpg");
            //downloadFromHDFS("input/testinput/ss.jpg","C:\\Users\\Administrator\\Pictures\\ss2.jpg");
    String src="D:\\workspace\\idea\\hadoopwordcount\\out\\artifacts\\hadoopwordcount_jar\\hadoopwordcount.jar";
    String des="input";
   // upFileToHDFS(src,des);
        listFile(des);

    }
}