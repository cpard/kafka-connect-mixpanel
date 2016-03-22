package org.apache.kafka.connect.mixpanel;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.*;

/**
 * Implementation of the Connector to pull event data from Mixpanel and store it into a Kafka topic.
 * Created by Kostas.
 */
public class MixPanelConnector extends SourceConnector {

    private static final String TOPIC_NAME = "topic";
    private static final String API_KEY = "api_key";
    private static final String API_SECRET = "api_secret";
    private static final String FROM_DATE = "from_date";

    private String topic;
    private String api_key;
    private String api_secret;
    private String from_date;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Starts the connector and check that the provider configuration is complete.
     */
    @Override
    public void start(Map<String, String> map) {

        topic = map.get(TOPIC_NAME);
        api_key = map.get(API_KEY);
        api_secret = map.get(API_SECRET);
        from_date = map.get(FROM_DATE);

        if (topic == null || topic.isEmpty())
            throw new ConnectException("MixPanelConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException("MixPanelConnector should only have a single topic when used as a source.");
        if(api_key == null || api_key.isEmpty())
            throw new ConnectException("MixPanelConnector configuration must include 'api key' setting");
        if(api_secret == null || api_secret.isEmpty())
            throw new ConnectException("MixPanelConnector configuration must include 'api secret' setting");
        if( from_date != null && !from_date.isEmpty() && !from_date.matches(DateUtils.dateRegExFormat))
            throw new ConnectException("MixPanelConnector requires dates of format yyyy-mm-dd for the from_date field");
    }

    /**
     * Create configurations for the tasks based on the current configuration of the Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MixPanelTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

            Map<String, String> config = new HashMap<>();
            config.put(TOPIC_NAME, topic);
            config.put(API_KEY, api_key);
            config.put(API_SECRET, api_secret);
            config.put(FROM_DATE, from_date);
            configs.add(config);

        return configs;
    }

    @Override
    public void stop() {

    }
}
