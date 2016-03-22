package org.apache.kafka.connect.mixpanel;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Kostas.
 */
public class MixPanelSourceConnectorTest {
    private MixPanelConnector connector;
    private ConnectorContext context;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new MixPanelConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put("topic", "mixpanelData");
        sourceProperties.put("api_key", "key123");
        sourceProperties.put("api_secret", "secret123");
        sourceProperties.put("from_date", "2016-03-17");

    }

    @Test
    public void testSourceTasks() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("mixpanelData", taskConfigs.get(0).get("topic"));
        Assert.assertEquals("key123", taskConfigs.get(0).get("api_key"));
        Assert.assertEquals("secret123", taskConfigs.get(0).get("api_secret"));
        Assert.assertEquals("2016-03-17", taskConfigs.get(0).get("from_date"));
        PowerMock.verifyAll();
    }
    //We just want to check that even if we ask for the creation of multiple tasks, we'll get just one config back
    @Test public void testMultipleSources(){
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(100);
        Assert.assertEquals(1, taskConfigs.size());
        PowerMock.verifyAll();
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        Assert.assertEquals(MixPanelTask.class, connector.taskClass());
        PowerMock.verifyAll();
    }
}
