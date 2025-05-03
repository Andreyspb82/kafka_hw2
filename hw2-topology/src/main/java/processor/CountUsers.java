package processor;

import model.UsersList;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

import static config.Constants.ALL_USERS;
import static config.Constants.BLOCKED_STORE;

/*
В этом классе составляю список все пользователей которые отправляют сообщения
Ключ для данного списка - "All-Users"
 */

public class CountUsers extends AbstractProcessor<String, String> {

    private KeyValueStore<String, UsersList> keyValueStore;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        keyValueStore = (KeyValueStore) context.getStateStore(BLOCKED_STORE);
    }

    @Override
    public void process(String key, String value) {

        UsersList oldAllUsersList = keyValueStore.get(ALL_USERS);

        if (oldAllUsersList == null) {
            List<String> newUsers = new ArrayList<>();
            newUsers.add(key);
            UsersList newUsersList = new UsersList();
            newUsersList.setUsers(newUsers);
            keyValueStore.put(ALL_USERS, newUsersList);
        } else {
            List<String> oldUsers = oldAllUsersList.getUsers();
            if (!oldUsers.contains(key)) {
                oldUsers.add(key);
                oldAllUsersList.setUsers(oldUsers);
                keyValueStore.put(ALL_USERS, oldAllUsersList);
            }
        }
    }
}

