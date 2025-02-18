import config.ProducerMessages;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/*
С помощью этого класса добавляю слова для цензуры.
Слово "цензура-2" будет исключено из перечня слов попадающих под цензуру (value - null)
Слово "цензура-3" будет добавлено в перечень слов попадающих под цензуру
Сообщения после добавления этих слов будут цензурироваться по новому перечню
 */

public class CreateMessages5CensureText2 {

    public static void main(String[] args) {

        ProducerMessages producer = new ProducerMessages();

        Map<String, String> censureMap = new HashMap<>();
        censureMap.put("цензура-1", "цензура-1");
        censureMap.put("цензура-2", null);
        censureMap.put("цензура-3", "цензура-3");

        for (String key : censureMap.keySet()) {
            ProducerRecord<String, String> record = new ProducerRecord<>("censure_text", key, censureMap.get(key));
            producer.sendMessage(record);
        }
    }
}
