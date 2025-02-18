import config.ProducerMessages;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

/*
Создаем сообщения которые проходят всю топологию без блокировки и цензурирования, так как
цензура и блокировка еще не добавлены в соответствующие топики.
Сообщения в топике "filtered_message" будут без блокировок и цензуры.
 */

public class CreateMessages1 {

    public static void main(String[] args) {

        ProducerMessages producer = new ProducerMessages();

        List<String> users = new ArrayList<>();
        users.add("User-1");
        users.add("User-2");
        users.add("User-3");
        users.add("User-4");
        users.add("User-5");
        users.add("User-6");
        users.add("User-7");

        for (String user : users) {
            for (int i = 1; i <= 1; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("messages", user,
                        "Test " + i + " цензура-1" + " цензура-2" + " цензура-3");
                producer.sendMessage(record);
            }
        }
    }
}
