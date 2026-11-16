package org.example;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.example.blockUser.BlockUser;
import org.example.blockUser.BlockUserSerdes;
import org.example.user.UserMessage;
import org.example.user.UserMessageSerdes;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class Main {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9094"; // было localhost:9092
    private static final String MESSAGE_TOPIC = "messages";
    private static final String BLOCKED_STORE = "blocked_users";
    private static final String FILTER_MESSAGE_TOPIC = "filtered_messages";
    public static StreamsBuilder builder = new StreamsBuilder();  // один builder на приложение
    private static final Set<String> EXTRA_WORDS = new HashSet<>(Arrays.asList("привет", "кафка"));

    public static void main(String[] args) {

        // Thread producerThread = new Thread(new MessageProducer(), "producer-thread");


        try {
            createTopics();

            produceBlockeUser();

            produceTestMessages();

            addGlobalKTable();

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "blocked-users-app");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            KafkaStreams streams = new KafkaStreams(builder.build(), props);

            CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }));
            streams.start();

            System.out.println("Приложение запущено, ждём сообщения... (Ctrl+C — стоп)");
            latch.await();

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
                NewTopic salesTopic = new NewTopic(MESSAGE_TOPIC, 1, (short) 1);

                adminClient.createTopics(Collections.singletonList(salesTopic)).all().get();
                System.out.println("Топик " + MESSAGE_TOPIC + " создан");
            } else {
                System.out.println("Топик " + MESSAGE_TOPIC + " уже существует");
            }

            // топик заблокированных — источник KTable
            if (!existingTopics.contains(BLOCKED_STORE)) {
                adminClient.createTopics(Collections.singletonList(
                        new NewTopic(BLOCKED_STORE, 1, (short) 1))).all().get();
                System.out.println("Топик " + BLOCKED_STORE + " создан");
            }

            // ← ИЗМЕНЕНО: вместо output-topic — топик для НЕ заблокированных сообщений
            if (!existingTopics.contains(FILTER_MESSAGE_TOPIC)) {
                adminClient.createTopics(Collections.singletonList(
                        new NewTopic(FILTER_MESSAGE_TOPIC, 1, (short) 1))).all().get();
                System.out.println("Топик " + FILTER_MESSAGE_TOPIC + " создан");
            }
        }
    }

    private static void produceTestMessages() {
        Properties producerProps = new Properties();
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // ← ИЗМЕНЕНО

        try (KafkaProducer<String, UserMessage> producer = new KafkaProducer<>(
                producerProps,
                new StringSerializer(),                // ключ
                new UserMessageSerdes().serializer()   // значение — объект UserMessage
        )) {
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key1",
                    new UserMessage(1, "Иван", "Иванов", "привет всем 3")));
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key2",
                    new UserMessage(2, "Анна", "Петрова", "привет всем 2")));
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key3",
                    new UserMessage(3, "Сергей", "Сидоров", "привет всем ")));
            producer.send(new ProducerRecord<>(MESSAGE_TOPIC, "key4",
                    new UserMessage(4, "Мария", "Кузнецова", "кафка")));

            producer.flush();
            System.out.println("Объекты UserMessage отправлены в " + MESSAGE_TOPIC);
        }
    }

    private static void produceBlockeUser() {
        Properties producerProps = new Properties();
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // ← ИЗМЕНЕНО

        try (KafkaProducer<String, BlockUser> producer = new KafkaProducer<>(
                producerProps,
                new StringSerializer(),                // ключ
                new BlockUserSerdes().serializer()     // значение — объект BlockUser
        )) {
            // ← ИЗМЕНЕНО: КЛЮЧ = id заблокированного (blockIdUser) строкой —
            // именно по этому ключу поток сообщений join с таблицей
            producer.send(new ProducerRecord<>(BLOCKED_STORE, "2", new BlockUser(1, 2)));
            producer.send(new ProducerRecord<>(BLOCKED_STORE, "3", new BlockUser(1, 3)));

            producer.flush();
            System.out.println("Объекты BlockUser отправлены в " + BLOCKED_STORE);
        }
    }

    //реализация KTable и кода с фильтрацией по пользователям и словам
    private static void addGlobalKTable() {
        GlobalKTable<String, BlockUser> globalTable = builder.globalTable(
                BLOCKED_STORE,
                Consumed.with(Serdes.String(), new BlockUserSerdes())
        );

        KStream<String, UserMessage> stream = builder.stream(
                MESSAGE_TOPIC,
                Consumed.with(Serdes.String(), new UserMessageSerdes())
        );

        KeyValueMapper<String, UserMessage, String> keyMapper =
                (key, value) -> String.valueOf(value.getId());


        ValueJoiner<UserMessage, BlockUser, UserMessage> joiner =
                (message, blocked) -> blocked == null ? message : null;

        KStream<String, UserMessage> notBlockedStream = stream.leftJoin(globalTable, keyMapper, joiner);

        notBlockedStream
                .filter((key, value) -> value != null)
                .mapValues(value -> new UserMessage(
                        value.getId(),
                        value.getName(),
                        value.getLastName(),
                        cleanText(value.getMessage())))
                .to(FILTER_MESSAGE_TOPIC, Produced.with(Serdes.String(), new UserMessageSerdes()));
    }


    private static String cleanText(String text) {
        if (text == null || text.trim().isEmpty()) {
            return text;
        }
        return Arrays.stream(text.split("\\s+"))
                .filter(word -> !EXTRA_WORDS.contains(word.toLowerCase()))
                .collect(Collectors.joining(" "));
    }
}