package com.imooc.bigdata.hdaoop.mr.wc;

import java.lang.String;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* KEYIN:Map任务读数据的key类型，offset ，是每行数据起始位置的偏移量，long
* VALUEIN Map任务读数据的value类型，其实就是一行行的字符串，String
*
* KEYOUT；map方法自定义实现输出的key类型,String
* VALUEOUT:map方法自定义实现输出的value类型, Integer
*
*long, String,  String,Integer是java里面的数据类型
* Hadoop自定义类型：序列化和反序列化（因为涉及传输）
* 故而，以上四种分别对应longWritable， text, IntWritable
* */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split(",");

        for (String word : words) {
           context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
