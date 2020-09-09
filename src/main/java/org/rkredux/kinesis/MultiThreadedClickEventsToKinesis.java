package org.rkredux.kinesis;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultiThreadedClickEventsToKinesis extends AbstractClickEventsToKinesis{

    //Just creates multiple instances of AmazonKinesisClient and run in parallel but still do
    //single HTTP requests for each record
    private final List<BasicClickEventsToKinesis> children;
    private final ExecutorService executor;

    public MultiThreadedClickEventsToKinesis(BlockingQueue<ClickEvent> inputQueue) {
        super(inputQueue);
        children = IntStream.range(0, 70)
                .mapToObj(i -> new BasicClickEventsToKinesis(inputQueue))
                .collect(Collectors.toList());
        executor = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
        children.forEach(c -> executor.submit(c));
    }

    @Override
    protected void runOnce() throws Exception {}

    //TODO implement this
    public long recordsPut() {
        return children.stream().mapToLong(BasicClickEventsToKinesis::recordsPut).sum();
    }

    @Override
    public void stop() {
        children.forEach(BasicClickEventsToKinesis::stop);
    }
}
