import config.ProducerMessages;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

/*
Добавляем в топик заблокированных пользователей
пользователей которых блокируем, где:
key - пользователь который блокирует
value - пользователь которого заблокировали
 */

public class CreateMessages2BlockedUsers {
    public static void main(String[] args) {

        ProducerMessages producer = new ProducerMessages();

        List<String> blockedUser1 = new ArrayList<>();
        blockedUser1.add("User-2");
        blockedUser1.add("User-3");

        List<String> blockedUser2 = new ArrayList<>();
        blockedUser2.add("User-1");

        List<String> blockedUser4 = new ArrayList<>();
        blockedUser4.add("User-3");


        for (String user : blockedUser1) {
            ProducerRecord<String, String> record = new ProducerRecord<>("blocked_users", "User-1", user);
            producer.sendMessage(record);
        }

        for (String user : blockedUser2) {
            ProducerRecord<String, String> record = new ProducerRecord<>("blocked_users", "User-2", user);
            producer.sendMessage(record);
        }

        for (String user : blockedUser4) {
            ProducerRecord<String, String> record = new ProducerRecord<>("blocked_users", "User-4", user);
            producer.sendMessage(record);
        }
    }
}
