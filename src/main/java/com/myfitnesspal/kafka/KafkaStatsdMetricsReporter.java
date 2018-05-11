package com.myfitnesspal.kafka;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricPredicate;

import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

/*
 * @author jackiewang518@gmail.com (Jackie Wang)
 */
public class KafkaStatsdMetricsReporter implements KafkaMetricsReporter,
	KafkaStatsdMetricsReporterMBean {

	static Logger LOG = Logger.getLogger(KafkaStatsdMetricsReporter.class);
	static String STATSD_DEFAULT_HOST = "localhost";
	static int STATSD_DEFAULT_PORT = 2003;
	static String STATSD_DEFAULT_PREFIX = "kafka";
	
	boolean initialized = false;
	boolean running = false;
	StatsdReporter reporter = null;
    String statsdHost = STATSD_DEFAULT_HOST;
    int statsdPort = STATSD_DEFAULT_PORT;
    String statsdGroupPrefix = STATSD_DEFAULT_PREFIX;
    MetricPredicate predicate = MetricPredicate.ALL;

	@Override
	public String getMBeanName() {
		return "kafka:type=com.myfitnesspal.kafka.KafkaStatsdMetricsReporter";
	}

	@Override
	public synchronized void startReporter(long pollingPeriodSecs) {
		if (initialized && !running) {
			reporter.start(pollingPeriodSecs, TimeUnit.SECONDS);
			running = true;
			LOG.info(String.format("Started Kafka Statsd metrics reporter with polling period %d seconds", pollingPeriodSecs));
		}
	}

	@Override
	public synchronized void stopReporter() {
		if (initialized && running) {
			reporter.shutdown();
			running = false;
			LOG.info("Stopped Kafka Statsd metrics reporter");
            try {
            	reporter = new StatsdReporter(
            			Metrics.defaultRegistry(),
            			statsdGroupPrefix,
            			predicate,
            			statsdHost,
            			statsdPort,
            			Clock.defaultClock()
            			);
            } catch (IOException e) {
            	LOG.error("Unable to initialize StatsdReporter", e);
            }
		}
	}

	@Override
	public synchronized void init(VerifiableProperties props) {
		if (!initialized) {
			KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
			statsdHost = props.getString("kafka.statsd.metrics.host", STATSD_DEFAULT_HOST).trim();
			statsdPort = props.getInt("kafka.statsd.metrics.port", STATSD_DEFAULT_PORT);
			statsdGroupPrefix = props.getString("kafka.statsd.metrics.group", STATSD_DEFAULT_PREFIX).trim();
            String regex = props.getString("kafka.statsd.metrics.exclude.regex", null);
			boolean reuseStatsdClient = props.getBoolean("kafka.statsd.metrics.client.reuse", true);


            LOG.debug("Initialize StatsdReporter ["+statsdHost+","+statsdPort+","+statsdGroupPrefix+"]");

            if (regex != null) {
            	predicate = new RegexMetricPredicate(regex);
            }
            try {
            	reporter = new StatsdReporter(
            			Metrics.defaultRegistry(),
            			statsdGroupPrefix,
            			predicate,
            			statsdHost,
            			statsdPort,
            			Clock.defaultClock()
            			);
            	reporter.setReuseStatsdClient(reuseStatsdClient);
            } catch (IOException e) {
            	LOG.error("Unable to initialize StatsdReporter", e);
            }
            if (props.getBoolean("kafka.statsd.metrics.reporter.enabled", false)) {
            	initialized = true;
            	startReporter(metricsConfig.pollingIntervalSecs());
                LOG.debug("StatsdReporter started.");
            }
        }
    }
}
