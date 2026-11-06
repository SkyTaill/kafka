package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.*;
//import org.example.user.User;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class SingleMessageConsumer implements Runnable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void run() {


        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094,localhost:9096");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        // отключаем автокоммит — коммитим вручную после обработки
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList("my-topic-2"));

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // десериализуем JSON-строку обратно в объект
//                        User order = MAPPER.readValue(record.value(), User.class);
//
//                        System.out.printf("Gets: key=%s, заказ=%s, partition=%d, offset=%d%n",
//                                record.key(), order, record.partition(), record.offset());

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
}
