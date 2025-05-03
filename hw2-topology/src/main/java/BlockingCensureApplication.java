import config.ConfigKafkaStreams;
import lombok.extern.slf4j.Slf4j;
import model.UsersList;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import processor.AddBlockedUsers;
import processor.AddCensureText;
import processor.BlockedUsers;
import processor.CensureMessage;
import processor.CountUsers;
import serde.StreamsSerdes;
import static config.Constants.BLOCKED_STORE;
import static config.Constants.CENSURE_STORE;

@Slf4j
public class BlockingCensureApplication {

    public static void main(String[] args) {
        try {

            ConfigKafkaStreams config = new ConfigKafkaStreams();

            //Создаю Serde для UsersList
            Serde<UsersList> usersListSerde = StreamsSerdes.UserListSerde();

            //Создаю хранилище для списков заблокированных пользователей, и общего списка пользователй
            KeyValueBytesStoreSupplier storeSupplierBlocked = Stores.persistentKeyValueStore(BLOCKED_STORE);
            StoreBuilder<KeyValueStore<String, UsersList>> builderBlocked = Stores.keyValueStoreBuilder(storeSupplierBlocked,
                    Serdes.String(), usersListSerde);

//          Создаю хранилище для хранения слов попадающих под цензуру
            KeyValueBytesStoreSupplier storeSupplierCensure = Stores.persistentKeyValueStore(CENSURE_STORE);
            StoreBuilder<KeyValueStore<String, String>> builderCensure = Stores.keyValueStoreBuilder(storeSupplierCensure,
                    Serdes.String(), Serdes.String());

            Topology topology = new Topology();

//          Создаю узел - источник сообщений
            topology.addSource("Message", "messages");
//          Создаю узел - источник списков заблокированных пользователей
            topology.addSource("BlockedUsersSource", "blocked_users");
//          Создаю узел - источник слов попадающих под цензуру
            topology.addSource("CensureTextSource", "censure_text");

//          Создаю узел - для составления списка всех пользователей
            topology.addProcessor("CountUsers", () -> new CountUsers(), "Message");

//          Создаю узел - для добавления слов попадающих под цензуру в хранилище
            topology.addProcessor("AddCensureText", () -> new AddCensureText(), "CensureTextSource");

//          Создаю узел - который цензурует входящие сообщения
            topology.addProcessor("CensureMessage", () -> new CensureMessage(), "Message");

//          Создаю список блокировок для каждого пользователя и общий список заблокированных пользователей
            topology.addProcessor("AddBlockedUsers", () -> new AddBlockedUsers(), "BlockedUsersSource");

//          Создаю узел - в котором, после цензуры, блокирую сообщения от пользователей из списка заблокированных пользователей
            topology.addProcessor("BlockedUsers", () -> new BlockedUsers(), "CensureMessage");

//          Создаю сток - в который отправляю сообщения после цензуры и блокировки
            topology.addSink("Sink", "filtered_message", "BlockedUsers");

//          Добавляю хранилище, слов попадающих под цензуру, в узлы в которых данное хранилище применяется
            topology.addStateStore(builderCensure, "AddCensureText", "CensureMessage");

//          Добавляю хранилище, списков заблокированных пользователей, в узлы в которых данное хранилище применяется
            topology.addStateStore(builderBlocked, "CountUsers", "AddBlockedUsers", "BlockedUsers");

            KafkaStreams kafkaStreams = new KafkaStreams(topology, config.setConfigKafkaStreams());

            kafkaStreams.start();
            log.info("Kafka Streams is working successfully.");

        } catch (Exception e) {
            log.warn("Error ", e.getMessage());
        }
    }
}
