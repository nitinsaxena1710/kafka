package kafka.consumer.streams;




import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.internals.KStreamAggProcessorSupplier;
import sun.awt.AWTAccessor;

import java.util.Properties;


public class StreamFilterTweets {


    public static void main(String args[]){
        //Create kafkaStreamConfigureProperties
        Properties kafkaStreamConfigureProperties = new Properties();
        kafkaStreamConfigureProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"Demo-kafka-streams");
        kafkaStreamConfigureProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.15:9092");
        kafkaStreamConfigureProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        kafkaStreamConfigureProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        //Create a topology
        StreamsBuilder builder = new StreamsBuilder();
        //Build the topology
        KStream<String,String> inputTopic = builder.stream("twitter_tweets");
        System.out.println("Topic "+inputTopic);
        System.out.println(inputTopic.toString());
        KStream<String,String> filteredStream=inputTopic.filter(
                (k,jsonTweet)-> extractUserFollowersInTweet(jsonTweet)>1000

        );
        filteredStream.to("important_tweets");
        //Start streams application

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),kafkaStreamConfigureProperties);

        // Start Streams Application
        kafkaStreams.start();


    }


    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersInTweet(String tweetJson){
        System.out.println("tweetJson "+tweetJson);
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }
        catch(Exception e ){
            e.printStackTrace();
            return 0;
        }

    }
}