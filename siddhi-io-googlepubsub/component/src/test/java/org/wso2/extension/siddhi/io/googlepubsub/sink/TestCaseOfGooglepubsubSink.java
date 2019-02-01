package org.wso2.extension.siddhi.io.googlepubsub.sink;

import com.google.cloud.pubsub.v1.Publisher;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.googlepubsub.util.ResultContainer;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;


public class TestCaseOfGooglepubsubSink {

    private static final Logger log = Logger.getLogger(TestCaseOfGooglepubsubSink.class);
    private volatile int count;
    private volatile boolean eventArrived;

    private static Publisher publisher;

    @BeforeMethod
    public void initBeforeMethod() {
        count = 0;
        eventArrived = false;
    }

        /**
         *Test to configure the GooglePubSub Sink publishes messages to a topic in  Google Pub Sub server.
         */

    @Test
    public void googlepubsubSimplePublishTest() throws Exception {
        log.info("Google Pub Sub Publish test. ");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        TestClient testClient = new TestClient("sp-path-1547649404768", "topicJ", "subJ", resultContainer);
        testClient.consumer();
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
        testClient.consumer();
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] {"World"});
            fooStream.send(new Object[] {"Class"});
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was  interrupted");
        }
        count = testClient.getCount();
        eventArrived = testClient.getEventArrived();

        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("World"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("Class"));
        siddhiAppRuntime.shutdown();
        testClient.shutdown();

    }

    /**
     * if a property missing from the siddhi stan sink which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test(expectedExceptions = { SiddhiAppValidationException.class })
    public void GooglePubSubPublishWithoutProjectidTest() {
        log.info("Google Pub Sub Publish without projectid test");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topicid = 'topicJ', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }


    /**
     * if a property missing from the siddhi stan sink which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test(expectedExceptions = { SiddhiAppValidationException.class })
    public void GooglePubSubPublishWithoutTopicidTest() {
        log.info("Google Pub Sub Publish without projectid test");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "projectid = 'sp-path-1547649404768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test to configure the Google Pub Sub publish messages to a new subscription.
     */

    @Test
    public void googlepubsubSimplePublishTestWithNewTopicid() throws Exception {
        log.info("Google Pub Sub Publish test. ");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        TestClient testClient = new TestClient("sp-path-1547649404768", "topicS", "subP", resultContainer);
        testClient.consumer();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topicid = 'topicS', "
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
        testClient.consumer();
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[] {"World"});
            fooStream.send(new Object[] {"Class"});
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was  interrupted");
        }
        count = testClient.getCount();
        eventArrived = testClient.getEventArrived();

        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("World"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("Class"));
        siddhiAppRuntime.shutdown();
        testClient.shutdown();

    }

}

