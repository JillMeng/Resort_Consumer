
import com.rabbitmq.client.*;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.JSONObject;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ResortConsumer {

    private final static String QUEUE_NAME = "Resort_Queue";
    private static final String EXCHANGE_NAME = "messages";


    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
//        factory.setUsername("radmin");
//        factory.setPassword("radmin");
//        factory.setHost("ec2-54-186-85-234.us-west-2.compute.amazonaws.com");
//        factory.setPort(5672);
        Connection connection = factory.newConnection();

        //connect to Jedis
        JedisPool pool = new JedisPool("localhost", 6379);
//        JedisPool pool = new JedisPool("52.43.194.35", 6379);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    //use exchange to consume the same message Publish/Subscribe
                    Channel channel = connection.createChannel();
                    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
                    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                    channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");
                    System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        //store resort date to Redis
                        //method 1
                        JSONObject jsonObject = new JSONObject(message);
                        int resortID = jsonObject.getInt("resortID");
                        String keyStr = String.valueOf(resortID);
                        //method 2
//                        JSONObject jsonObject = new JSONObject(message);
//                        String keyStr = jsonObject.getString("key");

                        //add jsonObject to Jedis
                        try (Jedis jedis = pool.getResource()) {
                            jedis.select(1);
                            jedis.set(keyStr, message);
//                            System.out.println("Message saved: " + jedis.get(keyStr));
                        }
                    };
                    // process messages
                    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
                } catch (IOException ex) {
                    Logger.getLogger(ResortConsumer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
        // start threads and block to receive messages
        for(int i=0; i<100; i++) {
            Thread recv =new Thread(runnable);
            recv.start();
        }
    }
}


