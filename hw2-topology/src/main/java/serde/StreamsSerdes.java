package serde;

import model.UsersList;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/*
Класс создания Serdes для класса UserList
 */

public class StreamsSerdes {

    public static Serde<UsersList> UserListSerde() {
        return new UserListSerde();
    }

    public static final class UserListSerde extends Serdes.WrapperSerde<UsersList> {

        public UserListSerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>(UsersList.class));
        }
    }

}
