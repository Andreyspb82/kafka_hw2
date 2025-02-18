package processor;

import model.UsersList;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

import static config.Constants.ALL_BLOCKED_USERS;
import static config.Constants.BLOCKED_STORE;

/*
В этом классе для каждого пользователя, который хочет добавить других пользователей в блокировку,
создаю в хранилище, в value список заблокированных пользователей.
Key - это автор этого списка.
Также на этом этапе создаю общий список всех заблокированных пользователей с ключом "All-Blocked-Users"
 */

public class AddBlockedUsers extends AbstractProcessor<String, String> {

    private KeyValueStore<String, UsersList> keyValueStore;

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        keyValueStore = (KeyValueStore) context.getStateStore(BLOCKED_STORE);
    }

    @Override
    public void process(String key, String value) {
        UsersList oldUsersList = keyValueStore.get(key);
        if (oldUsersList == null) {
            List<String> newUsers = new ArrayList<>();
            newUsers.add(value);
            UsersList newUserList = new UsersList();
            newUserList.setUsers(newUsers);
            keyValueStore.put(key, newUserList);
        } else {
            List<String> oldUsers = oldUsersList.getUsers();
            if (!oldUsers.contains(value)) {
                oldUsers.add(value);
                oldUsersList.setUsers(oldUsers);
                keyValueStore.put(key, oldUsersList);
            }
        }

        UsersList oldAllUsersBlockedList = keyValueStore.get(ALL_BLOCKED_USERS);
        if (oldAllUsersBlockedList == null) {
            List<String> newUsers = new ArrayList<>();
            newUsers.add(value);
            UsersList newAllUsersBlockedList = new UsersList();
            newAllUsersBlockedList.setUsers(newUsers);
            keyValueStore.put(ALL_BLOCKED_USERS, newAllUsersBlockedList);
        } else {
            List<String> oldUsers = oldAllUsersBlockedList.getUsers();
            if (!oldUsers.contains(value)) {
                oldUsers.add(value);
                oldAllUsersBlockedList.setUsers(oldUsers);
                keyValueStore.put(ALL_BLOCKED_USERS, oldAllUsersBlockedList);
            }
        }
    }
}
