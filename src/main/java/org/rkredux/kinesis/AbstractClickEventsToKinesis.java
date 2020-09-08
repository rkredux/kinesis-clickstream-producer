package org.rkredux.kinesis;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractClickEventsToKinesis implements Runnable{

    protected final static String STREAM_NAME = "testStream";
    protected final static String REGION = "us-east-1";
    protected final BlockingQueue<ClickEvent> inputQueue;
    protected volatile boolean shutDown = false;
    protected final AtomicLong recordsPut = new AtomicLong(0);

    //constructor of this abstract class
    protected AbstractClickEventsToKinesis(BlockingQueue<ClickEvent> inputQueue){
        this.inputQueue = inputQueue;
    }

    @Override
    public void run() {
        while(!shutDown){
            try {
                runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void stop(){
        shutDown = true;
    }

    protected abstract void runOnce() throws Exception;
}
