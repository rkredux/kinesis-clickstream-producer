package org.rkredux.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class BasicClickEventsToKinesis extends AbstractClickEventsToKinesis {

    //Uses the AmazonKinesis library not the KPL
    private final AmazonKinesis amazonKinesisClient;

    public BasicClickEventsToKinesis(BlockingQueue<ClickEvent> inputQueue) {
        super(inputQueue);
        final AmazonKinesisClientBuilder builder = AmazonKinesisClient.builder();
        builder.setRegion(REGION);
        amazonKinesisClient = builder.build();
    }

    @Override
    protected void runOnce() throws Exception {
        ClickEvent event = inputQueue.take();
        String partitionKey = event.getSessionId();
        ByteBuffer data = ByteBuffer.wrap(event.getPayload().getBytes("UTF-8"));
        amazonKinesisClient.putRecord(STREAM_NAME, data, partitionKey);
        recordsPut.getAndIncrement();
    }
}