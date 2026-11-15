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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;       // ← ДОБАВЛЕН импорт
import org.example.blockUser.BlockUser;
import org.example.blockUser.BlockUserSerdes;
import org.example.user.UserMessage;
import org.example.user.UserMessageSerdes;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9094"; // было localhost:9092
    private static final String MESSAGE_TOPIC = "messages";
    private static final String BLOCKED_STORE = "blocked_users";
    private static final String FILTER_MESSAGE_TOPIC = "filtered_messages";
    public static KStream<String, String> joinedStream;   // видно из main
    public static StreamsBuilder builder = new StreamsBuilder();  // ← ДОБАВЛЕНО: один builder на приложение


    public static void main(String[] args) {

        // Thread producerThread = new Thread(new MessageProducer(), "producer-thread");

        Thread singleConsumerThread = new Thread(new SingleMessageConsumer(), "single-consumer-thread");

        Thread batchConsumerThread = new Thread(new BatchMessageConsumer(), "batch-consumer-thread");

        try {
            createTopics();

            //produceTestMessages();
            //    produceBlockeUser();      // ← РАСКОММЕНТИРОВАНО: кладём список заблокированных в топик

            addGlobalKTable();        // строит топологию: таблица + поток + join + печать таблицы

            // ← ДОБАВЛЕНО: запуск (без него таблица никогда не наполнится)
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
                // Создаем топик для данных о продажах
                NewTopic salesTopic = new NewTopic(MESSAGE_TOPIC, 1, (short) 1);

                adminClient.createTopics(Collections.singletonList(salesTopic)).all().get();
                System.out.println("Топик " + MESSAGE_TOPIC + " создан");
            } else {
                System.out.println("Топик " + MESSAGE_TOPIC + " уже существует");
            }

            // ← ДОБАВЛЕНО: топик заблокированных — источник KTable
            if (!existingTopics.contains(BLOCKED_STORE)) {
                adminClient.createTopics(Collections.singletonList(
                        new NewTopic(BLOCKED_STORE, 1, (short) 1))).all().get();
                System.out.println("Топик " + BLOCKED_STORE + " создан");
            }

            // ← ДОБАВЛЕНО: выходной топик
            if (!existingTopics.contains("output-topic")) {
                adminClient.createTopics(Collections.singletonList(
                        new NewTopic("output-topic", 1, (short) 1))).all().get();
                System.out.println("Топик output-topic создан");
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

    private static void produceBlockeUser() {
        Properties producerProps = new Properties();
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094,localhost:9096");
        // сериализаторы передаём в конструктор: ключ — строка, значение — ваш Serdes

        try (KafkaProducer<String, BlockUser> producer = new KafkaProducer<>(
                producerProps,
                new StringSerializer(),                // ключ
                new BlockUserSerdes().serializer()   // значение — объект BlockUser
        )) {
            producer.send(new ProducerRecord<>(BLOCKED_STORE, "key1",
                    new BlockUser(1, 2)));
            producer.send(new ProducerRecord<>(BLOCKED_STORE, "key1",
                    new BlockUser(1, 3)));

            producer.flush();
            System.out.println("Объекты BlockUser отправлены в " + BLOCKED_STORE); // ← поправлен лог
        }
    }

    private static void addGlobalKTable() {
        // ← ЕДИНСТВЕННЫЙ источник для blocked_users — обычная KTable
        KTable<String, BlockUser> table = builder.table(
                BLOCKED_STORE,
                Consumed.with(Serdes.String(), new BlockUserSerdes())
        );

        // ← ваш "список заблокированных": каждое обновление таблицы печатается
        table.toStream().foreach((key, value) ->
                System.out.println("KTable: key=" + key
                        + ", idUser=" + value.getIdUser()
                        + ", blockIdUser=" + value.getBlockIdUser()));

        // ← поток событий берём ИЗ таблицы (никаких других источников этого топика нет)
        KStream<String, BlockUser> stream = table.toStream();

        // ← leftJoin с ОБЫЧНОЙ KTable: keyValueMapper НЕ нужен — ключи совпадают напрямую
        joinedStream = stream.leftJoin(
                table,
                (streamValue, tableValue) -> {
                    String left = streamValue.getIdUser() + "-" + streamValue.getBlockIdUser();
                    String right = (tableValue != null)
                            ? tableValue.getIdUser() + "-" + tableValue.getBlockIdUser()
                            : "нет_в_таблице";
                    return left + " => " + right;
                }
        );

        joinedStream.foreach((key, value) ->
                System.out.println("Joined Key: " + key + ", Value: " + value));
    }
}