package org.wso2.extension.siddhi.io.googlepubsub.sink;

import com.google.pubsub.v1.PubsubMessage;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.googlepubsub.util.ResultContainer;

/**
 *
 */
public class TestClient {
    private static final Logger log = Logger.getLogger(TestClient.class);
    private TestClient client = null;
    private boolean eventArrived;
    private int count;
    private  ResultContainer resultContainer;

    public void messageArrived(String s, PubsubMessage message) throws Exception {
        eventArrived = true;
        count++;
        resultContainer.eventReceived(message);
    }

    public int getCount() {
        return count;
    }

    public boolean getEventArrived() {
        return eventArrived;
    }


}


