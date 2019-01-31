package org.wso2.extension.siddhi.io.googlepubsub.util;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Util class to read the logs of test cases.
 */
public class UnitTestAppender extends AppenderSkeleton {
    private String messages;

    @Override
    protected void append(LoggingEvent loggingEvent) {
        messages = loggingEvent.getRenderedMessage();
    }

    @Override
    public void close() {

    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public String getMessages() {
        return messages;
    }
}
