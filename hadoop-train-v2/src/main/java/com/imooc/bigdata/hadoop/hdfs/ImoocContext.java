package com.imooc.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

public class ImoocContext {
    private Map<Object,Object> cacheMap = new HashMap<Object, Object>();
    public Map<Object, Object> getCacheMap(){
        return cacheMap;
    }
   /*
   * 写数据到缓存中去
   * @param Key 单词
   * @param Value 词频
   * */
    public void write (Object key, Object value) {
       cacheMap.put(key, value) ;
    }

    /**
     * 从缓存中取值
     * @param key
     * @return
     */
    public Object get(Object key){
        return cacheMap.get(key);

    }
}
