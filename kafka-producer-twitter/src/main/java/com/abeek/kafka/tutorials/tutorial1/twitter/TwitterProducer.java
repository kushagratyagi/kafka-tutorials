package com.abeek.kafka.tutorials.tutorial1.twitter;

import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //create twitter client
        Client client = new TwitterClient().createClient(msgQueue);
        client.connect();

        //create kafka producer

        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("tasks=run, message=" + e.getMessage());
                client.stop();
            }

            if(msg != null) {
                System.out.println("message=" + msg);
            }
        }
        LOGGER.info("End of Apllication");
    }
}
