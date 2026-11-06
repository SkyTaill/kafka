//package org.example;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.config.TopicConfig;
//import org.apache.kafka.common.serialization.StringSerializer;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import java.util.Properties;
//
//
//public class MessageProducer implements Runnable {
//
//    private static final String TOPIC = "my-topic-2";
//    private static final ObjectMapper MAPPER = new ObjectMapper();
//
//    @Override
//    public void run() {
//
//
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094,localhost:9096");
//        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Для синхронной репликации
//        props.put(ProducerConfig.RETRIES_CONFIG, 3);
//
//        props.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"); // Минимум 2 реплики должны подтвердить запись
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        props.put(ProducerConfig.RETRIES_CONFIG, 10);
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//            for (int i = 0; i < 10; i++) {
//                sendMessage(props, i);
//            }
//
//    }
//
//    private void sendMessage(Properties props, int id) {
//        User user = new User(id, "Maxim" + id, "Vr", 123123132L);
//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
//            String json = MAPPER.writeValueAsString(user);
//
//            //  по id пользователя выбираем партицию
//            ProducerRecord<String, String> record =
//                    new ProducerRecord<>(TOPIC, String.valueOf(user.getId()), json);
//
//            producer.send(record, (metadata, ex) -> {
//                if (ex != null) {
//                    System.err.println("Ошибка отправки: " + ex.getMessage());
//                } else {
//                    System.out.printf("Отправлено: topic=%s, partition=%d, offset=%d, json=%s%n",
//                            metadata.topic(), metadata.partition(), metadata.offset(), json);
//                }
//            });
//        } catch (Exception e) {
//            System.err.println("Продюсер упал: " + e.getMessage());
//        }
//    }
//}
