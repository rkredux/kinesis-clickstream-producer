package org.rkredux.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class BasicClickEventsToKinesis extends AbstractClickEventsToKinesis {

    private final AmazonKinesis amazonKinesisclient;

    public BasicClickEventsToKinesis(BlockingQueue<ClickEvent> inputQueue) {
        super(inputQueue);
        final AmazonKinesisClientBuilder builder = AmazonKinesisClient.builder();
        builder.setRegion(REGION);
        //so you get a kinesis client;you can use the APIs in the methods of this Class
        amazonKinesisclient = builder.build();
    }

    @Override
    protected void runOnce() throws Exception {
        //polls the queue for one single event
        ClickEvent event = inputQueue.take();
        String partitionKey = event.getSessionId();
        ByteBuffer data = ByteBuffer.wrap(event.getPayload().getBytes("UTF-8"));
        amazonKinesisclient.putRecord(STREAM_NAME, data, partitionKey);
        recordsPut.getAndIncrement();
    }
}