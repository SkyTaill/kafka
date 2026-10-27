package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.example.user.User;

import java.time.Duration;
import java.util.*;


public class BatchMessageConsumer implements Runnable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void run() {


        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094,localhost:9096");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Batch2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        // отключаем автокоммит — коммитим вручную после обработки
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList("my-topic-2"));
            List<ConsumerRecord<String, String>> buffer = new ArrayList<>(10);
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();


            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        buffer.add(record);
                        offsetsToCommit.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );

                        if (buffer.size() == 10) {
                            processBatch(buffer);                  // обрабатываем ровно 10
                            consumer.commitSync(offsetsToCommit);  // коммитим оффсеты всех затронутых партиций
                            buffer.clear();
                            offsetsToCommit.clear();
                        }

                    } catch (Exception e) {
                        System.err.printf("Битый JSON в offset=%d: %s%n",
                                record.offset(), e.getMessage());
                    }
                }

                // коммитим офсеты только после успешной обработки
                consumer.commitSync();
            }
        }
    }
    private static void processBatch(List<ConsumerRecord<String, String>> batch) {
        for (ConsumerRecord<String, String> record : batch) {
            System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                    record.partition(), record.offset(), record.key(), record.value());
        }
        System.out.println("10 сообщений обработано");
}
}
