package org.wso2.extension.siddhi.io.googlepubsub.source;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class PubSubMessageReceiver implements MessageReceiver {
    private static final Logger log = Logger.getLogger(MessageProcessor.class);
    private SourceEventListener sourceEventListener;
    private boolean paused;
    private ReentrantLock lock;
    private Condition condition;
    final BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
        messages.offer(pubsubMessage);
        ackReplyConsumer.ack();
    }

    public BlockingQueue<PubsubMessage> getMessages() {
        return messages;
    }
}
