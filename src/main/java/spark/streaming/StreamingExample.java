package spark.streaming;

/**
 * Created by mccstan on 02/05/17.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import redis.clients.jedis.Jedis;
import twitter4j.Status;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public class StreamingExample implements Serializable {

    public static void main(String[] args) throws Exception {

        // Jackson
        ObjectMapper mapper = new ObjectMapper();

        //Redis Provider
        Jedis jedis = new JedisProvider("localhost", 6379 ).RedisPublisher();

        //Gson
        Gson gson = new Gson();


        final String consumerKey = "YOUR CONSUMER KEY";
        final String consumerSecret = "YOUR CONSUMER SECRET";
        final String accessToken = "YOUR ACCESS TOKEN";
        final String accessTokenSecret = "YOUR ACCESS TOKEN SECRET";

        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("spark-streaming-twitter-redis")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("es.nodes", "localhost:9200")
                .set("es.index.auto.create", "true");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        //String[] filters = { "happy"};

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);


        //Process tweets texts
        //
        twitterStream
                .filter(tweets -> tweets.getGeoLocation() != null)
                .map(tweets -> tweets.getText())
                .map(words -> words.replaceAll("[^A-Za-z0-9\\s]", ""))
                .map(words -> words.toLowerCase())
                .map(words -> words.split("\\s+"))
                .map(words -> mapper.writeValueAsString(words))
                .foreachRDD(rdd ->
                        rdd.foreachPartition (pwords ->
                                Utils.asStream(pwords).forEach(pword ->
                                        Arrays.stream(pword
                                                .replaceAll("[^A-Za-z0-9,\\s]", "")
                                                .split(","))
                                                .forEach(word ->
                                                    new JedisProvider("localhost", 6379 ).RedisPublisher()
                                                        .publish("raw-messages", word)
                                                )
                                )
                        )
                );


        twitterStream
                .filter(tweets -> tweets.getGeoLocation() != null)
                .map(tweets -> new Tweet(tweets.getUser().getName(), tweets.getText(), tweets.getCreatedAt(), tweets.getHashtagEntities(), new Double[]{tweets.getGeoLocation().getLongitude(), tweets.getGeoLocation().getLatitude()}))
                .map(tweets -> mapper.writeValueAsString(tweets))
                .foreachRDD(rdd ->
                        rdd.collect().forEach(tweet ->
                                jedis.publish("tweets", gson.toJson(tweet))
                        )
                );


        jssc.start();
        jssc.awaitTermination();
    }


}
