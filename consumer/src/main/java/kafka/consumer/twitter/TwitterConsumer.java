package kafka.consumer.twitter;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;


import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Properties;


import static java.lang.Thread.*;


public class TwitterConsumer {

    private static String hostname ;
    private static String username ;
    private static String password ;



    private static final Logger LOG = LoggerFactory.getLogger(TwitterConsumer.class);
    //String jsonString = "{ \"foo\" : \"bar\"}";


    public static RestHighLevelClient createClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });



        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

    public static KafkaConsumer<String, String> createConsumer() {
        String bootStrapServers = "192.168.1.15:9092";
        String groupId = "kafka-demo-elasticsearch";
        String topic = "twitter_tweets";
        //System.out.println("Listening to Kafka Streams  ");
        //String topic = "important_tweets";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;


    }

    public static void main(String args[]) {
        System.out.println("NItin Saxena");
        //  String jsonString = "{ \"foo\" : \"bar\"}";
        RestHighLevelClient client = createClient();


        KafkaConsumer<String, String> consumer = createConsumer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            System.out.println(" Records Received " + records.count());
            for (ConsumerRecord<String, String> record : records) {
                String idTweet = exttractIDfromTweet(record.value());


                IndexRequest indexRequest;
                indexRequest = new IndexRequest("twitter", "tweets", idTweet).source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = null;
                try {
                    indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String id = indexResponse.getId();
                System.out.println(id);
                try {
                    sleep(10); // Introduce small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
            if(records.count()>0) {
                System.out.println("Commiting consumer ");
                consumer.commitSync();
            }
            try {
                sleep(1000); // Introduce small delay
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
   }

    private static JsonParser parser = new JsonParser();


    private static String exttractIDfromTweet(String tweetJson) {

        return parser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
        //      return
    }
}
