package com.imooc.bigdata.hadoop.hdfs;
/*使用HDFS API完成wordcount统计
* 需求：统计HDFS上的文件wc，然后将统计结果输出到HDFS
* 功能拆解：
* 1  读取HDFS上文件 ==>HDFS API
* 2  业务处理（词频统计）：对文件中的每一行数据都要进行业务处理（按照分隔符分割）==>Mapper
* 3   将处理结果缓存起来==> Context
*4   将结果输出到HDFS ==>HDFS API
* */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HDFSWC01 {
    public static void main(String[] args) throws Exception{

//    1  读取HDFS上文件 ==>HDFS API
        Path input= new Path("/hdfsapi/test/poem.txt");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"),new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator =  fs.listFiles(input,true);
        ImoocContext context = new ImoocContext();
        ImoocMapper mapper = new WordCountMapper();
        while(iterator.hasNext()){
            LocatedFileStatusoop var file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while((line = reader.readLine()) != null ){
               // 2  业务处理（词频统计）
                mapper.map(line,context);
            }
            reader.close();
            in.close();

        }

        // 3   将处理结果缓存起来  Map
        Map<Object,Object> contextmap = context.getCacheMap();
//4   将结果输出到HDFS ==>HDFS API
       Path output = new Path("/hdfsapi/test");

        FSDataOutputStream out = fs.create(new Path(output,"wc.txt"));
        Set<Map.Entry<Object,Object>>  entries = contextmap.entrySet();
        for(Map.Entry<Object,Object> entry:entries){
           out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
        }
        out.close();
        fs.close();
        System.out.println("---success !-----");
    }


}
