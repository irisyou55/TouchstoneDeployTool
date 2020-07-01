package Etcd;


import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Etcd操作工具类，包括信息插入、查询、删除、启动监听
 *  Created by You Shuhong on 2020/6/30
 */
public class EtcdUtil {
    public static Logger log = LoggerFactory.getLogger(EtcdUtil.class);
    //etcd客户端链接
    private static Client client = null;

    //链接初始化 单例模式
    public static synchronized Client getEtclClient(){
        if(null == client){
            client = Client.builder().endpoints("http://wqs97.com:2379").build();
        }
        return client;
    }
    /**
     * 根据指定的Key获取对应的value
     * @param key
     * @return
     * @throws Exception
     */
    public static String getEtcdValueByKey(String key) throws Exception {
        List<KeyValue> kvs = EtcdUtil.getEtclClient().getKVClient().get(ByteSequence.fromString(key)).get().getKvs();
        if(kvs.size()>0){
            String value = kvs.get(0).getValue().toStringUtf8();
            return value;
        }
        else {
            return null;
        }
    }
    /**
     * 新增或者修改指定的Key
     * @param key
     * @param value
     * @return
     */
    public static void putEtcdValueByKey(String key,String value) throws Exception{
        EtcdUtil.getEtclClient().getKVClient().put(ByteSequence.fromString(key),ByteSequence.fromBytes(value.getBytes("utf-8")));
    }
    /**
     * 删除指定的Key
     * @param key
     * @return
     */
    public static void deleteEtcdValueByKey(String key){
        EtcdUtil.getEtclClient().getKVClient().delete(ByteSequence.fromString(key));
    }
    /**
     * etcd的监听，监听指定的key，当key 发生变化后，监听自动感知到变化。
     * @param key 指定需要监听的key
     */
    public static void initListen(String key){
        try {
            //加载配置
            getConfig(EtcdUtil.getEtclClient().getKVClient().get(ByteSequence.fromString(key)).get().getKvs());
            new Thread(() -> {
                Watch.Watcher watcher = EtcdUtil.getEtclClient().getWatchClient().watch(ByteSequence.fromString(key));
                try {
                    while(true) {
                        watcher.listen().getEvents().stream().forEach(watchEvent -> {
                            KeyValue kv = watchEvent.getKeyValue();
                            log.info("etcd event:{} ,changed key is:{},Changed Value is:{}",watchEvent.getEventType(),
                                    kv.getKey().toStringUtf8(),kv.getValue().toStringUtf8());
                        });
                    }
                } catch (InterruptedException e) {
                    log.info("etcd listen start cause Exception:{}",e);
                }
            }).start();
        } catch (Exception e) {
            log.info("etcd listen start cause Exception:{}",e);
        }
    }
    private static String getConfig(List<KeyValue> kvs){
        if(kvs.size()>0){
            String config = kvs.get(0).getKey().toStringUtf8();
            String value = kvs.get(0).getValue().toStringUtf8();
            log.info("etcd 's config 's config key is :{},value is:{}",config,value);
            return value;
        }
        else {
            return null;
        }
    }

    
}


