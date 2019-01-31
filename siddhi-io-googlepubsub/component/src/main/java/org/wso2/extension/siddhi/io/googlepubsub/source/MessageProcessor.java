package org.wso2.extension.siddhi.io.googlepubsub.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class MessageProcessor {
    private static final Logger log = Logger.getLogger(MessageProcessor.class);
    private SourceEventListener sourceEventListener;
    private boolean paused;
    private ReentrantLock lock;
    private Condition condition;

    public MessageProcessor(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

}
