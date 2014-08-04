Kafka StatsD Metrics Reporter
==============================


Install On Broker
------------

1. Build the `kafka-statsd-1.0.0.jar` jar using `mvn package`.
2. Add `kafka-statsd-1.0.0.jar` to the `libs/` 
   directory of your kafka broker installation
3. Configure the broker (see the configuration section below)
4. Restart the broker

Configuration
------------

Edit the `server.properties` file of your installation, activate the reporter by setting:

    kafka.metrics.reporters=com.myfitnesspal.kafka.KafkaStatsdMetricsReporter[,kafka.metrics.KafkaCSVMetricsReporter[,....]]
    kafka.statsd.metrics.reporter.enabled=true

Here is a list of default properties used:

    kafka.statsd.metrics.host=localhost
    kafka.statsd.metrics.port=8125
    kafka.statsd.metrics.group=kafka
    # This can be use to exclude some metrics from statsd 
    # since kafka has quite a lot of metrics, it is useful
    # if you have many topics/partitions.
    kafka.statsd.metrics.exclude.regex=<not set>

Usage As Lib
-----------

Simply build the jar and publish it to your maven internal repository (this 
package is not published to any public repositories unfortunately).
