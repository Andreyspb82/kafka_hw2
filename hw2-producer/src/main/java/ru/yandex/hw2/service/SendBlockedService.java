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
public class SendBlockedService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC_BLOCKED = "blocked_users";

    public void sendBlocked(List<Message> blockedList) {

        if (blockedList.isEmpty()) {
            log.info("List<message> is empty");
        } else {
            for (Message blocked : blockedList) {
                kafkaTemplate.send(TOPIC_BLOCKED, blocked.getKey(), blocked.getValue());
                log.info("Blocked: key={}, value= {}", blocked.getKey(), blocked.getValue());
            }
        }
    }
}
