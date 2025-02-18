package processor;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import static config.Constants.CENSURE_STORE;

/*
В этом классе сохраняю в хранилище цензурируемые слова.
 */

public class AddCensureText extends AbstractProcessor<String, String> {

    private KeyValueStore<String, String> keyValueStore;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        keyValueStore = (KeyValueStore) context.getStateStore(CENSURE_STORE);
    }

    @Override
    public void process(String key, String value) {
        keyValueStore.put(key, value);
    }
}
