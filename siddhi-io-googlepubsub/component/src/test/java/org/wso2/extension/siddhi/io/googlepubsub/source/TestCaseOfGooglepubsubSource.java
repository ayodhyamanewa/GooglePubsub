package org.wso2.extension.siddhi.io.googlepubsub.source;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfGooglepubsubSource {
    private static Logger log = Logger.getLogger(TestCaseOfGooglepubsubSource.class);
    private AtomicInteger count = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;
    private volatile boolean eventArrived;
    private List<String> receivedEventNameList;
    private static SubscriptionAdminClient subscriptionAdminClient;
    private static Subscriber subscriber;


    @BeforeMethod
    public void initBeforeMethod() {
        count.set(0);
        eventArrived = false;
    }

    @BeforeClass
    private void initializeDockerContainer() throws InterruptedException {
        count.set(0);

    }

    /**
     * Test the ability to subscripe to a Google Pub Sub topic and .and receive messages.
     */
    @Test
    public void testGooglePubSubSourceEvent() {
        log.info("Test to receive messages");
        receivedEventNameList = new ArrayList<>(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "projectid = 'sp-path-1547649404768', "
                        + "topicid = 'topicJ', "
                        + "subscriptionid = 'subJ', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                }
            }
        });
        siddhiAppRuntimeSource.start();
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topicid = 'topicJ', "
                        + "projectid = 'sp-path-1547649404768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[]{"Hiii"});
            SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        siddhiAppRuntimeSource.shutdown();
        siddhiAppRuntime.shutdown();
    }


}


