package com.imooc.bigdata.hadoop.hdfs;
import org.apache.commons.lang.StringUtils;
public class WordCountMapper implements ImoocMapper {

    @Override
    public void map(String line, ImoocContext context) {
        String[] words = line.split(",");
        for (String word :words){
           Object value = context.get(word);
           if ( value == null){//没有出现过该单词
              context.write(word,1);
           }else{
               int v = Integer.parseInt(value.toString());
               context.write(word,v + 1);//取出单词对应的次数
           }

        }

    }
}
