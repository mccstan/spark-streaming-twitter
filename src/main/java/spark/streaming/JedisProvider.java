package spark.streaming;

/**
 * Created by mccstan on 02/05/17.
 */

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;

public class JedisProvider implements Serializable{

    private JedisPool pool;
    public JedisProvider(String host, int port){
        pool = new JedisPool(host, port);
    }
    public Jedis RedisPublisher(){
        return pool.getResource();
    }
}
