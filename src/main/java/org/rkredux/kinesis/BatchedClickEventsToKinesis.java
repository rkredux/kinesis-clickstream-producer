package org.rkredux.kinesis;

import java.util.concurrent.BlockingQueue;

public class BatchedClickEventsToKinesis extends AbstractClickEventsToKinesis{

    //implements putRecords API using the Kinesis Producer Library

    public BatchedClickEventsToKinesis(BlockingQueue<ClickEvent> inputQueue) {
        super(inputQueue);
    }

    @Override
    protected void runOnce() throws Exception {

    }
}
