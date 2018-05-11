package com.myfitnesspal.kafka;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Locale;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import com.yammer.metrics.core.MetricPredicate;

import java.lang.Thread.State;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map.Entry;

/**
 * A simple reporter which sends out application metrics to a statsd server
 * periodically.
 * 
 * @author jackiewang518@gmail.com (Jackie Wang)
 */
public class StatsdReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
	private static final Logger LOG = Logger.getLogger(StatsdReporter.class);

	private static enum StatType {
		COUNTER, TIMER, GAUGE
	}
    private static final int STATSD_BUFFER = 512;
	protected final String prefix;
	protected final MetricPredicate predicate;
	protected final Locale locale = Locale.US;
	protected final Clock clock;
	protected final String host;
	protected final int port;
	private boolean reuseStatsdClient = true;
	protected final VirtualMachineMetrics vm;
	protected StatsdClient statsdClient;
	public boolean printVMMetrics = true;

	/**
	 * Enables the statsd reporter to send data for the default metrics registry
	 * to statsd server with the specified period.
	 * 
	 * @param period
	 *            the period between successive outputs
	 * @param unit
	 *            the time unit of {@code period}
	 * @param host
	 *            the host name of statsd server
	 * @param port
	 *            the port number on which the statsd server is listening
	 */
	public static void enable(long period, TimeUnit unit, String host, int port) {
		enable(Metrics.defaultRegistry(), period, unit, host, port);
	}

	/**
	 * Enables the statsd reporter to send data for the given metrics registry
	 * to statsd server with the specified period.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param period
	 *            the period between successive outputs
	 * @param unit
	 *            the time unit of {@code period}
	 * @param host
	 *            the host name of statsd server
	 * @param port
	 *            the port number on which the statsd server is listening
	 */
	public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port) {
		enable(metricsRegistry, period, unit, host, port, null);
	}

	/**
	 * Enables the statsd reporter to send data to statsd server with the
	 * specified period.
	 * 
	 * @param period
	 *            the period between successive outputs
	 * @param unit
	 *            the time unit of {@code period}
	 * @param host
	 *            the host name of statsd server
	 * @param port
	 *            the port number on which the statsd server is listening
	 * @param prefix
	 *            the string which is prepended to all metric names
	 */
	public static void enable(long period, TimeUnit unit, String host, int port, String prefix) {
		enable(Metrics.defaultRegistry(), period, unit, host, port, prefix);
	}

	/**
	 * Enables the statsd reporter to send data to statsd server with the
	 * specified period.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param period
	 *            the period between successive outputs
	 * @param unit
	 *            the time unit of {@code period}
	 * @param host
	 *            the host name of statsd server
	 * @param port
	 *            the port number on which the statsd server is listening
	 * @param prefix
	 *            the string which is prepended to all metric names
	 */
	public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port,
			String prefix) {
		enable(metricsRegistry, period, unit, host, port, prefix, MetricPredicate.ALL);
	}

	/**
	 * Enables the statsd reporter to send data to statsd server with the
	 * specified period.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param period
	 *            the period between successive outputs
	 * @param unit
	 *            the time unit of {@code period}
	 * @param host
	 *            the host name of statsd server
	 * @param port
	 *            the port number on which the statsd server is listening
	 * @param prefix
	 *            the string which is prepended to all metric names
	 * @param predicate
	 *            filters metrics to be reported
	 */
	public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port,
			String prefix, MetricPredicate predicate) {
		try {
			final StatsdReporter reporter = new StatsdReporter(metricsRegistry, prefix, predicate, host, port,
					Clock.defaultClock());
			reporter.start(period, unit);
		} catch (Exception e) {
			LOG.error("Error creating/starting statsd reporter:", e);
		}
	}

	/**
	 * Creates a new {@link StatsdReporter}.
	 * 
	 * @param host
	 *            the host name of statsd server
	 * @param port
	 *            the port number on which the statsd server is listening
	 * @param prefix
	 *            is prepended to all names reported to statsd
	 * @throws IOException
	 *             if there is an error connecting to the statsd server
	 */
	public StatsdReporter(String host, int port, String prefix) throws IOException {
		this(Metrics.defaultRegistry(), host, port, prefix);
	}

	/**
	 * Creates a new {@link StatsdReporter}.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param host
	 *            the host name of statsd server
	 * @param port
	 *            the port number on which the statsd server is listening
	 * @param prefix
	 *            is prepended to all names reported to statsd
	 * @throws IOException
	 *             if there is an error connecting to the statsd server
	 */
	public StatsdReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix) throws IOException {
		this(metricsRegistry, prefix, MetricPredicate.ALL, host, port, Clock.defaultClock());
	}

	/**
	 * Creates a new {@link StatsdReporter}.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param prefix
	 *            is prepended to all names reported to statsd
	 * @param predicate
	 *            filters metrics to be reported
	 * @param clock
	 *            a {@link Clock} instance
	 * @throws IOException
	 *             if there is an error connecting to the statsd server
	 */
	public StatsdReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, String host,
			int port, Clock clock) throws IOException {
		this(metricsRegistry, prefix, predicate, host, port, clock, VirtualMachineMetrics.getInstance());
	}

	/**
	 * Creates a new {@link StatsdReporter}.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param prefix
	 *            is prepended to all names reported to statsd
	 * @param predicate
	 *            filters metrics to be reported
	 * @param host
	 *            is the host of statsd server
	 * @param port
	 *            is the port of statsd server
	 * @param clock
	 *            a {@link Clock} instance
	 * @param vm
	 *            a {@link VirtualMachineMetrics} instance
	 * @throws IOException
	 *             if there is an error connecting to the statsd server
	 */
	public StatsdReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, String host,
			int port, Clock clock, VirtualMachineMetrics vm) throws IOException {
		this(metricsRegistry, prefix, predicate, host, port, clock, vm, "statsd-reporter");
	}

	/**
	 * Creates a new {@link StatsdReporter}.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param prefix
	 *            is prepended to all names reported to statsd
	 * @param predicate
	 *            filters metrics to be reported
	 * @param host
	 *            is the host of statsd server
	 * @param port
	 *            is the port of statsd server
	 * @param clock
	 *            a {@link Clock} instance
	 * @param vm
	 *            a {@link VirtualMachineMetrics} instance
	 * @throws IOException
	 *             if there is an error connecting to the statsd server
	 */
	public StatsdReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, String host,
			int port, Clock clock, VirtualMachineMetrics vm, String name) throws IOException {
		super(metricsRegistry, name);
		this.host = host;
		this.port = port;
		this.vm = vm;

		this.clock = clock;

		if (prefix != null) {
			// Pre-append the "." so that we don't need to make anything
			// conditional later.
			this.prefix = prefix + ".";
		} else {
			this.prefix = "";
		}
		this.predicate = predicate;
	}

	@Override
	public void run() {
		try {
			if (statsdClient == null || reuseStatsdClient == false) {
				statsdClient = new StatsdClient(host, port, STATSD_BUFFER);
			}
			final long epoch = clock.time() / 1000;
			if (this.printVMMetrics) {
				printVmMetrics(epoch);
			}
			printRegularMetrics(epoch);
		} catch (Exception e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Error writing to Statsd", e);
			} else {
				LOG.warn("Error writing to Statsd: " + e.getMessage());
			}
		} finally {

		}
	}

	@Override
	public void shutdown() {
		try {
			if (statsdClient != null) {
				statsdClient.close();
			}
		} catch (IOException ioe) {
			LOG.error("Fail to shutdown statsdClient", ioe);
		}
	}

	protected void sendStatsd(String name, String value, StatType statType) throws IOException {
		String statTypeStr = "";
		switch (statType) {
		case COUNTER:
			statTypeStr = "c";
			break;
		case GAUGE:
			statTypeStr = "g";
			break;
		case TIMER:
			statTypeStr = "ms";
			break;
		}
		final String messageFormat = "%s%s:%s|%s\n";
		statsdClient.doSend(String.format(locale, messageFormat, prefix, sanitizeString(name), value, statTypeStr));
	}

	protected void printRegularMetrics(final Long epoch) {
		for (Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(predicate)
				.entrySet()) {
			for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
				final Metric metric = subEntry.getValue();
				if (metric != null) {
					try {
						metric.processWith(this, subEntry.getKey(), epoch);
					} catch (Exception ignored) {
						LOG.error("Error printing regular metrics:", ignored);
					}
				}
			}
		}
	}

	protected void sendInt(String name, String valueName, long value) throws IOException {
		sendStatsd(name + "." + valueName, format(value), StatType.GAUGE);
	}

	protected void sendFloat(String name, String valueName, double value) throws IOException {
		sendStatsd(name + "." + valueName, format(value), StatType.GAUGE);
	}

	protected String sanitizeName(MetricName name) {
		final StringBuilder sb = new StringBuilder().append(name.getGroup()).append('.').append(name.getType())
				.append('.');
		if (name.hasScope()) {
			sb.append(name.getScope()).append('.');
		}
		return sb.append(name.getName()).toString();
	}

	protected String sanitizeString(String s) {
		return s.replace(' ', '-');
	}

	@Override
	public void processGauge(MetricName name, Gauge<?> gauge, Long epoch) throws IOException {
		sendStatsd(sanitizeName(name), gauge.value().toString(), StatType.GAUGE);
	}

	@Override
	public void processCounter(MetricName name, Counter counter, Long epoch) throws IOException {
		sendStatsd(sanitizeName(name) + "count", format(counter.count()), StatType.COUNTER);
	}

	@Override
	public void processMeter(MetricName name, Metered meter, Long epoch) throws IOException {
		final String sanitizedName = sanitizeName(name);
		sendStatsd(sanitizedName + ".count", format(meter.count()), StatType.GAUGE);
		sendStatsd(sanitizedName + ".m1_rate", format(meter.oneMinuteRate()), StatType.TIMER);
		sendStatsd(sanitizedName + ".m5_rate", format(meter.fiveMinuteRate()), StatType.TIMER);
		sendStatsd(sanitizedName + ".m15_rate", format(meter.fifteenMinuteRate()), StatType.TIMER);
		sendStatsd(sanitizedName + ".mean_rate", format(meter.meanRate()), StatType.TIMER);
	}

	@Override
	public void processHistogram(MetricName name, Histogram histogram, Long epoch) throws IOException {
		final String sanitizedName = sanitizeName(name);
		sendSummarizable(sanitizedName, histogram);
		sendSampling(sanitizedName, histogram);
	}

	@Override
	public void processTimer(MetricName name, Timer timer, Long epoch) throws IOException {
		processMeter(name, timer, epoch);
		final String sanitizedName = sanitizeName(name);
		sendSummarizable(sanitizedName, timer);
		sendSampling(sanitizedName, timer);
	}

	protected void sendSummarizable(String sanitizedName, Summarizable metric) throws IOException {
		sendStatsd(sanitizedName + ".count", format(metric.sum()), StatType.GAUGE);
		sendStatsd(sanitizedName + ".max", format(metric.max()), StatType.TIMER);
		sendStatsd(sanitizedName + ".mean", format(metric.mean()), StatType.TIMER);
		sendStatsd(sanitizedName + ".min", format(metric.min()), StatType.TIMER);
		sendStatsd(sanitizedName + ".stddev", format(metric.stdDev()), StatType.TIMER);
	}

	protected void sendSampling(String sanitizedName, Sampling metric) throws IOException {
		final Snapshot snapshot = metric.getSnapshot();
		sendStatsd(sanitizedName + ".p50", format(snapshot.getMedian()), StatType.TIMER);
		sendStatsd(sanitizedName + ".p75", format(snapshot.get75thPercentile()), StatType.TIMER);
		sendStatsd(sanitizedName + ".p95", format(snapshot.get95thPercentile()), StatType.TIMER);
		sendStatsd(sanitizedName + ".p98", format(snapshot.get98thPercentile()), StatType.TIMER);
		sendStatsd(sanitizedName + ".p99", format(snapshot.get99thPercentile()), StatType.TIMER);
		sendStatsd(sanitizedName + ".p999", format(snapshot.get999thPercentile()), StatType.TIMER);
	}

	protected void printVmMetrics(long epoch) throws IOException {
		sendFloat("jvm.memory", "heap_usage", vm.heapUsage());
		sendFloat("jvm.memory", "non_heap_usage", vm.nonHeapUsage());
		for (Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
			sendFloat("jvm.memory.memory_pool_usages", sanitizeString(pool.getKey()), pool.getValue());
		}

		sendInt("jvm", "daemon_thread_count", vm.daemonThreadCount());
		sendInt("jvm", "thread_count", vm.threadCount());
		sendInt("jvm", "uptime", vm.uptime());
		sendFloat("jvm", "fd_usage", vm.fileDescriptorUsage());

		for (Entry<State, Double> entry : vm.threadStatePercentages().entrySet()) {
			sendFloat("jvm.thread-states", entry.getKey().toString().toLowerCase(), entry.getValue());
		}

		for (Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
			final String name = "jvm.gc." + sanitizeString(entry.getKey());
			sendInt(name, "time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
			sendInt(name, "runs", entry.getValue().getRuns());
		}
	}

	private String format(long n) {
		return String.format(locale, "%d", n);
	}

	private String format(double v) {
		return String.format(locale, "%2.2f", v);
	}

	public void setReuseStatsdClient(boolean reuseStatsdClient) {
		this.reuseStatsdClient = reuseStatsdClient;
	}

	private class StatsdClient {
		private ByteBuffer sendBuffer;
		private InetSocketAddress address;
		private DatagramChannel channel;

		public StatsdClient(String host, int port, int packetBufferSize) throws IOException {
			sendBuffer = ByteBuffer.allocate(packetBufferSize);
			address = new InetSocketAddress(InetAddress.getByName(host), port);
			channel = DatagramChannel.open();
		}

		public void close() throws IOException {
			// save any remaining data to StatsD
			flush();

			// Close the channel
			if (channel.isOpen()) {
				channel.close();
			}

			sendBuffer.clear();
		}

		public void doSend(String stats) {
			final byte[] data = stats.getBytes(Charset.forName("UTF-8"));

			// If we're going to go past the threshold of the buffer then
			// flush.
			// the +1 is for the potential '\n' in multi_metrics below
			if (sendBuffer.remaining() < (data.length + 1)) {
				flush();
			}

			// multiple metrics are separated by '\n'
			if (sendBuffer.position() > 0) {
				sendBuffer.put("\n".getBytes(Charset.forName("UTF-8")));
			}

			// append the data
			sendBuffer.put(data);
		}

		private void flush() {
			try {
				int sizeOfBuffer = sendBuffer.position();
				if (sizeOfBuffer <= 0) {
					// empty buffer
					return;
				}

				// send and reset the buffer
				sendBuffer.flip();
				int nbSentBytes = channel.send(sendBuffer, address);
				LOG.debug("Send entirely stat :" + new String(sendBuffer.array(), Charset.forName("UTF-8")) + " to host "
						+ address.getHostName() + " : " + address.getPort());
				if (sizeOfBuffer != nbSentBytes) {
					LOG.error("Could not send entirely stat (" + sendBuffer.toString() + ") : "
							+  new String(sendBuffer.array(), Charset.forName("UTF-8")) + " to host "
							+ address.getHostName() + " : " + address.getPort() + ". Only sent " + nbSentBytes
							+ " out of " + sizeOfBuffer);
				}
				sendBuffer.limit(sendBuffer.capacity());
				sendBuffer.rewind();
			} catch (IOException e) {
				LOG.error("Could not send entirely stat (" + sendBuffer.toString() + ") : "
						+  new String(sendBuffer.array(), Charset.forName("UTF-8")) + " to host "
						+ address.getHostName() + " : "	+ address.getPort(), e);
			}
		}
	}
}
