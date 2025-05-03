package ru.yandex.hw2.service;//package service;

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
public class SendMessageService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC_MESSAGE = "messages";

    public void sendMessage(List<Message> messages) {

        if (messages.isEmpty()) {
            log.info("List<message> is empty");
        } else {
            for (Message message : messages) {
                kafkaTemplate.send(TOPIC_MESSAGE, message.getKey(), message.getValue());
                log.info("Message: key={}, value= {}", message.getKey(), message.getValue());
            }
        }
    }
}
