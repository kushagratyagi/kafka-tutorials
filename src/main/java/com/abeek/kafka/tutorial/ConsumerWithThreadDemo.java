package com.abeek.kafka.tutorial;

import com.abeek.kafka.tutorial.tasks.ConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreadDemo {
    private static Logger LOGGER = LoggerFactory.getLogger(ConsumerWithThreadDemo.class.getName());
    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerThread = new ConsumerThread(latch);
        Thread thread = new Thread(consumerThread);
        thread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            LOGGER.info("task=addShutdownHook, message=now calling ConsumerThread.shutdown()");
            ((ConsumerThread)consumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("task=addShutdownHook, message=Application exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException");
        } finally {
            LOGGER.info("task=main, message=Application stopping");
        }
    }
}
