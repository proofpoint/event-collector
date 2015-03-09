package com.proofpoint.event.collector;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.event.collector.queue.Queue;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Created by bhawkins on 2/18/15.
 */
public class KafkaBatchProcessor implements BatchProcessor<Event>
{
    private static final Logger log = Logger.get(KafkaBatchProcessor.class);
    private final BatchHandler<Event> handler;
    private final int maxBatchSize;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Duration throttleTime;
    private ConsumerThread consumerThread;

    public KafkaBatchProcessor(String eventType, String flowId, BatchHandler<Event> handler, BatchProcessorConfig config)
    {
        checkNotNull(eventType, "eventType is null");
        checkNotNull(flowId, "flowId is null");
        checkNotNull(handler, "handler is null");

        this.handler = handler;
        this.maxBatchSize = checkNotNull(config, "config is null").getMaxBatchSize();
        this.executor = newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(format("batch-processor-%s", name)).build());
        throttleTime = checkNotNull(config.getThrottleTime(), "throttle time is null");

        this.consumerThread = new ConsumerThread();
    }
    @Override
    public void start()
    {

        executor.schedule(consumerThread);
    }

    @Override
    public void stop()
    {

    }

    @Override
    public void put(Event entry)
    {
        //Should never get called
    }

    private class ConsumerThread implements Runnable
    {
        public static final Logger logger = Logger.get(ConsumerThread.class);

        private final Datastore m_datastore;
        private final String m_topic;
        private final KafkaStream<byte[], byte[]> m_stream;
        private final int m_threadNumber;
        private TopicParser m_topicParser;

        public ConsumerThread(Datastore datastore, String topic, KafkaStream<byte[], byte[]> stream, int threadNumber)
        {
            m_datastore = datastore;
            m_topic = topic;
            m_stream = stream;
            m_threadNumber = threadNumber;
        }

        public void setTopicParser(TopicParser parser)
        {
            m_topicParser = parser;
        }

        @Override
        public void run()
        {
            Thread.currentThread().setName(this.m_topic + "-" + this.m_threadNumber);
            logger.info("starting consumer thread " + this.m_topic + "-" + this.m_threadNumber);
            for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : m_stream)
            {
                try
                {
                    logger.debug("message: " + messageAndMetadata.message());
                    DataPointSet set = m_topicParser.parseTopic(m_topic, messageAndMetadata.key(),
                            messageAndMetadata.message());

                    for (DataPoint dataPoint : set.getDataPoints())
                    {
                        m_datastore.putDataPoint(set.getName(), set.getTags(), dataPoint);
                    }

                    //m_counter.incrementAndGet();
                }
                catch (DatastoreException e)
                {
                    // TODO: rewind to previous message to provide consistent consumption
                    logger.error("Failed to store datapoints: ", e);
                }
                catch (Exception e)
                {
                    logger.error("Failed to parse message: " + messageAndMetadata.message(), e.getMessage());
                }
            }
        }
    }
}
