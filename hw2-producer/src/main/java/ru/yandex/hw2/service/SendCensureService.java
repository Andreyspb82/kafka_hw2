package ru.yandex.hw2.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.hw2.model.Message;

import java.util.List;

@Service
@Slf4j
@Data
public class SendCensureService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC_CENSURE = "censure_text";

    public void sendCensure(List<Message> censures) {

        if (censures.isEmpty()) {
            log.info("List<message> is empty");
        } else {
            for (Message censure : censures) {
                kafkaTemplate.send(TOPIC_CENSURE, censure.getKey(), censure.getValue());
                log.info("Censure: key={}, value= {}", censure.getKey(), censure.getValue());
            }
        }
    }

}
