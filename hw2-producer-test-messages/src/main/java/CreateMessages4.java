import config.ProducerMessages;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

/*
После отправки сообщений из этого класса, сообщения в топике "filtered_message"
пройдут цензуру по словам "цензура-1" "цензура-2".
Также сообщения будут заблокированы согласно списку заблокированных пользователей
 */

public class CreateMessages4 {
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
            for (int i = 10; i <= 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("messages", user,
                        "Test " + i + " цензура-1" + " цензура-2" + " цензура-3");
                producer.sendMessage(record);
            }
        }
    }
}
