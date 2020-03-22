package com.imooc.bigdata.hdaoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
/*
* Reducer 和 Mapper中其实使用到了什么设计模式： 模板*/
public class WordCOuntReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
       while (iterator.hasNext()){
           IntWritable value = iterator.next();
           count  += value.get();//把hadoop数据类型转换成java类型，在进行相加计算
       }
       context.write(key, new IntWritable(count));//把java数据类型再转换成Hadoop的数据类型，已给hadoop处理
    }
}
