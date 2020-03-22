package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.xpath.XPath;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/*使用JAVA API操作HDFS文件系统
 * 关键点：
 * 1 创建Configuration
 * 2 获取 FileSystem
 * 3  ... 就是你的HDFS API操作*/
public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://localhost:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("----------setUp--------------");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @Test
    /*创建HDFS文件夹*/
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    /*读取text文件*/
    public void text() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/user/hadoop/README.md"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    @Test
    /*写入文件*/
    public void create() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        out.writeUTF("BAD BAD GUIGU");
        out.flush();
        out.close();


    }
    @Test
    public void rename ()throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        Boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);

    }
/*拷贝本地文件到HDFS文件系统*/
    @Test
    public void copyFormLocalFile() throws Exception{
        Path src = new Path("/home/hadoop/桌面/铁路.txt");
        Path dst = new Path("/hdfsapi/test/c.txt");
        fileSystem.copyFromLocalFile(src, dst);
    }
    /*拷贝大文件*/
    @Test
    public void copyFormLocalBigFile() throws Exception{
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/home/hadoop/桌面/凶冥十杀阵.txt")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/d.txt"), new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(in,out,4096);
    }
    /*从HDFS考回本地*/
    @Test
    public void copyToLocalFile()throws Exception{

        Path dst = new Path("/home/hadoop/桌面/o.txt");
        Path src  = new Path("/hdfsapi/test/c.txt");
        fileSystem.copyToLocalFile(src,dst);
    }
    /*查看目标文件夹下的所有文件*/
    @Test
    public void listFiles() throws Exception{
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
       for (FileStatus file : statuses) {
           Path path = file.getPath();
           String isdir = file.isDirectory() ? "文件夹":"文件";
           short block_replication = file.getReplication();
           long blocksize= file.getBlockSize();
           long modification_time= file.getModificationTime();
           FsPermission permission  = file.getPermission();
           System.out.println(path.toString()+"\t"+isdir+"\t"+block_replication+"\t"+
                   modification_time+"\t"+permission);

       }

    }
    /*递归地查看目标文件夹下的所有文件*/
    @Test
    public void listFilesRecursive() throws Exception{
        RemoteIterator<LocatedFileStatus> files =  fileSystem.listFiles(new Path("/hdfsapi/test"),true);
        while (files.hasNext()){
            LocatedFileStatus file = files.next();
            Path path = file.getPath();
            String isdir = file.isDirectory() ? "文件夹":"文件";
            short block_replication = file.getReplication();
            long blocksize= file.getBlockSize();
            long modification_time= file.getModificationTime();
            FsPermission permission  = file.getPermission();
            System.out.println(path.toString()+"\t"+isdir+"\t"+block_replication+"\t"+
                    modification_time+"\t"+permission);

        }


    }
    /*查看文件快信息`*/
    @Test
    public void getFileBlockLocatiions()throws Exception{
       FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/d.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());
        for(BlockLocation block : blocks){
           for (String name:block.getNames()){
              System.out.println(name + " : " + block.getOffset()+ " : "+block.getLength());
           }
        }
    }
    /*删除文件*/
    @Test
    public void delete ()throws Exception{
        boolean result  = fileSystem.delete(new Path("/hdfsapi/test/b.txt"),true) ;
        System.out.println(result);

    }
    @After
    public void tearDown() {
        System.out.println("----------tearDown--------------");
        fileSystem = null;
        configuration = null;
    }
//    public static void main(String[] args) throws Exception {
//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration, "hadoop");
//        Path path = new Path("/hdfsapi/test");
//        boolean result = fileSystem.mkdirs(path);
//        System.out.println(result);
//    }
}
