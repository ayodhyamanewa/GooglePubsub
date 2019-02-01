package org.wso2.extension.siddhi.io.googlepubsub.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Class to retain results received by google pub sub client so that tests can poll the result and assert against.
 */
public class ResultContainer {
    private static Log log = LogFactory.getLog(ResultContainer.class);
    private int eventCount;
    private List<String> results;
    private CountDownLatch latch;
    private int timeout = 90;

    public ResultContainer(int expectedEventCount) {
        eventCount = 0;
        results = new ArrayList<>(expectedEventCount);
        latch = new CountDownLatch(expectedEventCount);
    }

    public ResultContainer(int expectedEventCount, int timeoutInSeconds) {
        eventCount = 0;
        results = new ArrayList<>(expectedEventCount);
        latch = new CountDownLatch(expectedEventCount);
        timeout = timeoutInSeconds;
    }

    public void eventReceived(String message) {
        eventCount++;
        results.add(message);
        latch.countDown();
    }

    public void waitForResult() {
        try {
            latch.await(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

//    public void eventReceived(PubsubMessage message) throws UnsupportedEncodingException {
//        eventCount++;
//        String message1 = new String(message.getData().toStringUtf8());
//        results.add(message1);
//        latch.countDown();
//    }

    public Boolean assertMessageContent(String content) {
        try {
            if (latch.await(timeout, TimeUnit.SECONDS)) {
                for (String message : results) {
                    if (message.contains(content)) {
                        return true;
                    }
                }
                return false;
            } else {
                log.error("Expected number of results not received. Only received " + eventCount + " events.");
                return false;
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}

