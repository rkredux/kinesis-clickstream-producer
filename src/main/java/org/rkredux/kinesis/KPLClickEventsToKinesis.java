package org.rkredux.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class KPLClickEventsToKinesis<KinesisProducer> extends AbstractClickEventsToKinesis {

    //uses the Kinesis producer library but uses putUserRecord which is not the same as putRecords
    //it buffers it so there is some batching but single HTTP requests because of putUserRecord
    private final KinesisProducer kinesisProducer;

    public KPLClickEventsToKinesis(BlockingQueue<ClickEvent> inputQueue) {
        super(inputQueue);
        kinesisProducer = new KinesisProducer(new KinesisProducerConfiguration()
                .setRegion(REGION)
                .setRecordMaxBufferedTime(RECORD_MAX_BUFFERED_TIME)
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
