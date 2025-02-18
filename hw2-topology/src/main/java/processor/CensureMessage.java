package processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static config.Constants.CENSURE;
import static config.Constants.CENSURE_STORE;

/*
В этом классе происходит цензурированние сообщений.
Из хранилища извлекаются все слова у которых value != null
Проходим по списку извлеченных слов и проверяем их наличие в value входного сообщения.
Если в сообщении есть слово попадающие под цензуру, оно заменяется значением ****
 */

public class CensureMessage extends AbstractProcessor<String, String> {

    private KeyValueStore<String, String> keyValueStore;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        keyValueStore = (KeyValueStore) context.getStateStore(CENSURE_STORE);
    }

    @Override
    public void process(String key, String value) {


        KeyValueIterator<String, String> iterator = keyValueStore.all();

        List<String> censureList = new ArrayList<>();

        while (iterator.hasNext()) {
            KeyValue<String, String> entry = iterator.next();
            censureList.add(entry.value);
        }
        iterator.close();
        context.forward(key, censure(value, censureList));
    }

    private String censure(String value, List<String> censureList) {
        for (String censure : censureList) {
            Pattern pattern = Pattern.compile(censure, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(value);
            value = matcher.replaceAll(CENSURE);
        }
        return value;
    }
}
