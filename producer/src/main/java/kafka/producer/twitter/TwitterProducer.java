package kafka.producer.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.omg.CORBA.TIMEOUT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.awt.AWTAccessor;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
public class TwitterProducer
{

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());


    String consumerKey;
    String consumerSecret;
    String token;
    String secret ;

    public static void main(String args[]){
        new TwitterProducer().run();
        System.out.println("Nitin Test ...2 ");
    }


    public KafkaProducer<String,String> createKafkaProducer(){
        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.15:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        return producer;
    }
    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String,String > kafkaProducer = createKafkaProducer();
        /*Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println(" Shut Down Hook ");
        }));*/
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
// TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println("TwitterProducer.run() msg " + msg);
            kafkaProducer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(null!=e){
                        System.out.println("Something bad happened");
                    }
                }
            });

        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        System.out.println("TwitterProducer.run() ..1 ");


        //BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
//
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
//List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList( " cricket ","Man vs Wild ");
//hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("kda4koWfI3mOXiodHtUC3cPfy", "6tZ7A6GlHi5NFW3Q6Bv0bLZiiGJjNvjoqwsY2Fqy7VZwYtVcw7", "1158353890326171648-t2lCgBFxAfw9bjfGr5usOWuugBcFJL", "UmXQFMTPIFJZ3LCePjdiLCGaqKLottPF7yJLRNeH3sJTB");


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}


