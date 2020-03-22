package com.imooc.bigdata.hdaoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/*
* Driver； 设置Mapper和Reducer的相关属性
*
*对本地文件进行统计，将统计结果输出到本地
*  */
public class WordCountLocationApp {
    public static void main(String[] args)throws Exception {
        Configuration configuration = new Configuration();
        //创建一个Job
        Job job = Job.getInstance(configuration);
        //设置Job对应的参数：主类
        job.setJarByClass(WordCountLocationApp.class);
        //设置Job对应的参数：设置自定义Mapper和Reducer的处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCOuntReducer.class);
        //设置Job对应的参数：设置Mapper输出的key和value的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Job对应的参数：设置Reducer输出的key和value的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置Job对应的参数：作业的输入输出路径
        FileInputFormat.setInputPaths(job,new Path("input"));
        FileOutputFormat.setOutputPath(job,new Path("output"));

        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);


    }
}
