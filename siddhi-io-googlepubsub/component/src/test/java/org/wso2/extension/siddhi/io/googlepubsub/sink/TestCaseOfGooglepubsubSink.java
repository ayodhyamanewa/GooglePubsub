package org.wso2.extension.siddhi.io.googlepubsub.sink;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.googlepubsub.util.ResultContainer;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;


import java.util.concurrent.TimeUnit;

public class TestCaseOfGooglepubsubSink {

    private static final Logger log = Logger.getLogger(TestCaseOfGooglepubsubSink.class);
    private static TopicAdminClient topicAdminClient;
    private volatile int count;
    private volatile boolean eventArrived;

    private static Publisher publisher;

    @BeforeMethod
    public void initBeforeMethod() {
        count = 0;
        eventArrived = false;
    }

    @AfterClass
    public static void stop() {
        try {
            if (topicAdminClient != null) {
                topicAdminClient.shutdown();
            }
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }

        } catch (Exception e) {
            log.error("Error in disconnecting the publisher.");
        }
    }


        /**
         *Test to configure the GooglePubSub Sink publishes messages to a topic in  Google Pub Sub server.
         */

    @Test
    public void googlepubsubSimplePublishTest() {
        log.info("Google Pub Sub Publish test. ");
        ResultContainer resultContainer = new ResultContainer(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topicid = 'topicJ', "
                        + "projectid = 'sp-path-1547649404768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count++;
                }
            }
        });
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] {"Fine"});
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was  interrupted");
        }
        AssertJUnit.assertEquals(1, count);
       // AssertJUnit.assertTrue(resultContainer.assertMessageContent("Fine"));
        siddhiAppRuntime.shutdown();

    }
}

