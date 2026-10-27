package org.example;


public class Main {

    public static void main(String[] args) {

        Thread producerThread = new Thread(new MessageProducer(), "producer-thread");

        Thread singleConsumerThread = new Thread(new SingleMessageConsumer(), "single-consumer-thread");

        Thread batchConsumerThread = new Thread(new BatchMessageConsumer(), "batch-consumer-thread");

        producerThread.start();
        singleConsumerThread.start();
        batchConsumerThread.start();

        try {
            producerThread.join();
            singleConsumerThread.join();
            batchConsumerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Application finished");
    }
}
