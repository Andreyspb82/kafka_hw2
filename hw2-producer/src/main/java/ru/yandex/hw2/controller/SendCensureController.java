package ru.yandex.hw2.controller;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.hw2.model.Message;
import ru.yandex.hw2.service.SendCensureService;

import java.util.List;

@RestController
@Slf4j
@RequestMapping(path = "/censure")
@AllArgsConstructor
public class SendCensureController {

    private final SendCensureService sendCensureService;

    @PostMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public void sendMessage(@RequestBody List<Message> censures) {

        log.info("POST/ censures: {}", censures);
        sendCensureService.sendCensure(censures);
    }
}
