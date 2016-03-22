# kafka-connect-mixpanel
The connector loads your events data from Mixpanel to Kafka. 

# Building
You can build the connector with Maven:
```
mvn clean
mvn package
```

## Sample Configuration
```ini
name=mixpanel-connector
connector.class=org.apache.kafka.connect.mixpanel.MixPanelConnector
tasks.max=1
api_key = YOUR_MIXPANEL_API_KEY
api_secret = YOUR_MIXPANEL_SECRET_KEY
from_date = 2016-02-01
topic=mixp
```
* **name**: name of the connector.
* **connector.class**: class of the implementation of the connector.
* **tasks.max**: maximum number of tasks to create. Even if you put a number greater than one here it will be ignored by the implementation.
* **api_key**: your API key for the project from which you want to pull data from.
* **api_secret**: your API secret for the project from which you want to pull data from.
* **from_date**: Some date in the past from when you would like to start pulling data.
* **topic**: the name of the Kafka topic where the data will be pushed.