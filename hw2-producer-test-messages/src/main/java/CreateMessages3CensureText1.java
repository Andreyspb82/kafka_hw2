import config.ProducerMessages;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/*
С помощью этого класса добавляю слова для цензуры.
Сообщения после добавления этих слов будут цензурироваться.
Для того чтобы можно было не только добавлять, но и
удалять сообщения из списка цензуры, для key и value передаю одни и те же значения чтобы добавить слово.
Для того чтобы удалить слово из перечня, в качестве value необходимо передать null
 */

public class CreateMessages3CensureText1 {
    public static void main(String[] args) {

        ProducerMessages producer = new ProducerMessages();

        Map<String, String> censureMap = new HashMap<>();
        censureMap.put("цензура-1", "цензура-1");
        censureMap.put("цензура-2", "цензура-2");

        for (String key : censureMap.keySet()) {
            ProducerRecord<String, String> record = new ProducerRecord<>("censure_text", key, censureMap.get(key));
            producer.sendMessage(record);
        }
    }
}
