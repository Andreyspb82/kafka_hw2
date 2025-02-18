package config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

@Slf4j
public class ProducerMessages {

    ConfigProducerMessages configProducerMessages = new ConfigProducerMessages();

    public void sendMessage(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(configProducerMessages.setProducerConfig());
        try {
            producer.send(record);
            log.info("Message sent: key = {}, value = {}",
                    record.key(), record.value());
        } catch (KafkaException ex) {
            log.warn("Error ", ex);
        } finally {
            producer.close();
        }
    }
}
