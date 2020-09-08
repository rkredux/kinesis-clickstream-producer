package org.rkredux.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class KPLClickEventsToKinesis extends AbstractClickEventsToKinesis {

    private final KinesisProducer kinesisProducer;

    public KPLClickEventsToKinesis(BlockingQueue<ClickEvent> inputQueue) {
        super(inputQueue);
        kinesisProducer = new KinesisProducer(new KinesisProducerConfiguration()
                .setRegion(REGION)
                .setRecordMaxBufferedTime(5000)
        );
    }

    @Override
    protected void runOnce() throws Exception {
        ClickEvent event = inputQueue.take();
        String partitionKey = event.getSessionId();
        ByteBuffer data = ByteBuffer.wrap(
                event.getPayload().getBytes("UTF-8"));
        while (kinesisProducer.getOutstandingRecordsCount() > 5e4) {
            Thread.sleep(1);
        }
        kinesisProducer.addUserRecord(STREAM_NAME, partitionKey, data);
        recordsPut.getAndIncrement();
    }
}
