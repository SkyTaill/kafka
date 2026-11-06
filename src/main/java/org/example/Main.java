package org.example;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.user.UserMessage;
import org.example.user.UserMessageSerdes;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class Main {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9094"; // было localhost:9092
    private static final String MESSAGE_TOPIC = "messages";
    private static final String BLOCKED_STORE_NAME = "blocked_users";
    private static final String FILTER_MESSAGE_TOPIC = "filtered_messages";

    public static void main(String[] args) {

       // Thread producerThread = new Thread(new MessageProducer(), "producer-thread");

        Thread singleConsumerThread = new Thread(new SingleMessageConsumer(), "single-consumer-thread");

        Thread batchConsumerThread = new Thread(new BatchMessageConsumer(), "batch-consumer-thread");

        try {
            createTopics();

            produceTestMessages();

            Properties props = new Properties();
            // props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-router-example");
//            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

//            producerThread.start();
//            singleConsumerThread.start();
//            batchConsumerThread.start();
//
//            try {
//                producerThread.join();
//                singleConsumerThread.join();
//                batchConsumerThread.join();
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
            System.out.println("Application finished");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Создает необходимые топики в Kafka
     */
    private static void createTopics() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Проверяем существование топика
            Set<String> existingTopics = adminClient.listTopics().names().get();

            if (!existingTopics.contains(MESSAGE_TOPIC)) {
                // Создаем топик для данных о продажах
                NewTopic salesTopic = new NewTopic(MESSAGE_TOPIC, 1, (short) 1);

                adminClient.createTopics(Collections.singletonList(salesTopic)).all().get();
                System.out.println("Топик " + MESSAGE_TOPIC + " создан");
            } else {
                System.out.println("Топик " + MESSAGE_TOPIC + " уже существует");
            }
        }
    }

    private static void produceTestMessages() {
        Properties producerProps = new Properties();
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094,localhost:9096");
        // сериализаторы передаём в конструктор: ключ — строка, значение — ваш Serdes

        try (KafkaProducer<String, UserMessage> producer = new KafkaProducer<>(
                producerProps,
                new StringSerializer(),                // ключ
                new UserMessageSerdes().serializer()   // значение — объект UserMessage
        )) {
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key1",
                    new UserMessage(1, "Иван", "Иванов", "100")));
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key2",
                    new UserMessage(2, "Анна", "Петрова", "200")));
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key3",
                    new UserMessage(3, "Сергей", "Сидоров", "привет")));
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key4",
                    new UserMessage(4, "Мария", "Кузнецова", "кафка")));

            producer.flush();
            System.out.println("Объекты UserMessage отправлены в " + MESSAGE_TOPIC);
        }
    }
}
