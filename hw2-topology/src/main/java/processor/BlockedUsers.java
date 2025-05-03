package processor;

import model.UsersList;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

import static config.Constants.ALL_BLOCKED_USERS;
import static config.Constants.ALL_USERS;
import static config.Constants.BLOCKED_STORE;

/*
В этом классе сравниваются 2 списка "All-Users" и "All-Blocked-Users"
дальше проходят сообщения только от тех пользователей которых нет в списке "All-Blocked-Users"
 */

public class BlockedUsers extends AbstractProcessor<String, String> {

    private KeyValueStore<String, UsersList> keyValueStore;


    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        keyValueStore = (KeyValueStore) context.getStateStore(BLOCKED_STORE);
    }

    @Override
    public void process(String key, String value) {

        UsersList allUsers = keyValueStore.get(ALL_USERS);
        UsersList blockedUsers = keyValueStore.get(ALL_BLOCKED_USERS);

        if(blockedUsers == null) {
            context.forward(key, value);
        } else  {
            List<String> allUsersList = allUsers.getUsers();
            List<String> blockedUsersList = blockedUsers.getUsers();
            allUsersList.removeAll(blockedUsersList);
            if (allUsersList.contains(key)) {
                context.forward(key, value);
            }
        }
    }

}
