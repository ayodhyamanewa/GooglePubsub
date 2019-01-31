package org.wso2.extension.siddhi.io.googlepubsub.source;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.googlepubsub.util.GooglePubSubConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Source configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type =  "Supported parameter types.
 *                                        eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                          according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type =   "Supported parameter types.
 *                                         eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                         according to the type."),
 * },
 * //If Source system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first  system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "the default value of the system parameter.",
 *                                      possibleParameter="the possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax = "sample query with Source annotation that explain how extension use in Siddhi."
 *                              description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */

@Extension(
        name = "googlepubsub",
        namespace = "source",
        description = "A Google Pub Sub receives events to be processed by WSO2 SP from a topic.",
        parameters = {
                @Parameter(
                        name = GooglePubSubConstants.GOOGLEPUBSUB_SERVER_PROJECTID,
                        description = "The unique ID of the project within which the topic is created. ",
                        type = DataType.STRING
                ),

                @Parameter(
                        name = GooglePubSubConstants.TOPIC_ID,
                        description = "The unique id of the topic from which the events are received." +
                                "Only one topic must be specified.",
                        type = DataType.STRING
                ),

                @Parameter(
                        name = GooglePubSubConstants.SUBSCRIPTION_ID,
                        description = "The unique id of the subscription from which messages should be retrieved." +
                                "This subscription id connects the topic to a subscriber application that receives " +
                                "and processes messages published to the topic. A topic can have multiple " +
                                "subscriptions,but a given subscription belongs to a single topic.",
                        type = DataType.STRING
                ),


                @Parameter(
                        name = GooglePubSubConstants.GOOGLEPUBSUB_IDLETIMEOUT,
                        description = "Idle timeout of the connection.",
                        type = DataType.INT,
                        optional = true, defaultValue = "50"),


        },
        examples = {
                @Example(description = "This example shows how to subscribe to a googlepubsub topic with all " +
                        "supporting configurations.With the following configuration the identified source,will " +
                        "subscribe to a topic named as topicA which resides in a googlepubsub instance with" +
                        "the project id of 'my-project-test-227508'.This Google Pub Sub source configuration listens" +
                        " to the googlepubsub topicA.The events are received in the text format and mapped to a " +
                        "Siddhi event,and sent to a the Test stream.",
                        syntax = "@source(type='googlepubsub',@map(type='text'),"
                                + "topicId='topicA',"
                                + "projectId='sp-path-1547649404768',"
                                + "subscriptionId='subB',"
                                + ")\n"
                                + "define stream Test(name String);"

                )
        }
)

// for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sources
public class GooglepubsubSource extends Source {
    private static final Logger log = Logger.getLogger(GooglepubsubSource.class);

    private SourceEventListener sourceEventListener;
    private OptionHolder optionHolder;
    private String projectid;
    private String topicid;
    private String subscriptionid;
    private SiddhiAppContext siddhiAppContext;
    private String siddhiAppName;
    private Object configReader;
    private SubscriptionAdminClient subscriptionAdminClient;
    private ProjectSubscriptionName subscriptionName;
    private MessageProcessor messageProcessor;
    private Subscriber subscriber;

    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get Siddhi
     *                            related utility functions.
     */
    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {

        this.siddhiAppContext = siddhiAppContext;
        this.sourceEventListener = sourceEventListener;
        this.optionHolder = optionHolder;
        this.siddhiAppName = siddhiAppContext.getName();
        this.configReader = configReader;
        this.siddhiAppName = siddhiAppContext.getName();
        this.subscriptionid = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.SUBSCRIPTION_ID);
        this.topicid = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.TOPIC_ID);
        this.projectid = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.GOOGLEPUBSUB_SERVER_PROJECTID);
        this.messageProcessor = new MessageProcessor(sourceEventListener);

    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    /**
     * Initially Called to connect to the end point for start retrieving the messages asynchronously .
     *
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection. (can be used when events are receiving asynchronously)
     * @throws ConnectionUnavailableException if it cannot connect to the source backend immediately.
     */
    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        GoogleCredentials credentials = null;
        File credentialsPath = new File("/home/ayodhya/Desktop/Google/siddhi-io-googlepubsub/component/" +
                "src/main/resources/sp.json");  // TODO: update to your key path.
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (FileNotFoundException e) {
            log.error("The file that points to your service account credentials is not found.");
        } catch (IOException e) {
            log.error("Credentials are missing.");
        }

        final BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();

        class MessageReceiverExample implements MessageReceiver {

            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                try {
                    sourceEventListener.onEvent("message :" + "\"" + message.getData().toStringUtf8()
                                    + "\"" + " ", null);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to pull messages from topic");
                }
                consumer.ack();
            }
        }

        ExecutorProvider executorProvider =
                InstantiatingExecutorProvider.newBuilder()
                        .setExecutorThreadCount(1)
                        .build();

        ProjectTopicName topicName = ProjectTopicName.of(projectid, topicid);

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(
                projectid, subscriptionid);
        try {
            // create a pull subscription with default acknowledgement deadline (= 10 seconds)
            SubscriptionAdminSettings subscriptionAdminSettings =
                    SubscriptionAdminSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();
            SubscriptionAdminClient subscriptionAdminClient =
                    SubscriptionAdminClient.create(subscriptionAdminSettings);
            //  SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create();
            // create a pull subscription with default acknowledgement deadline (= 10 seconds)
            Subscription subscription =
                    subscriptionAdminClient.createSubscription(
                            subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
        } catch (ApiException e) {
            // example : code = ALREADY_EXISTS(409) implies subscription already exists

        } catch (IOException e) {
            log.error("");
        }

        //System.out.printf(
        // "Subscription %s:%s created.\n",
        //subscriptionName.getProject(), subscriptionName.getSubscription());

        Subscriber subscriber = null;
        try {
            // create a subscriber bound to the asynchronous message receiver
            subscriber =
                    Subscriber.newBuilder(subscriptionName, new MessageReceiverExample())
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).
                            setExecutorProvider(executorProvider)
                            .build();
            subscriber.addListener(
                    new Subscriber.Listener() {
                        @Override
                        public void failed(Subscriber.State from, Throwable failure) {
                        }
                    }, MoreExecutors.directExecutor());
            subscriber.startAsync().awaitRunning();
            // why should I put sleep here??
            //Thread.sleep(60000);
        } catch (Exception e) {
        }
//        finally {
//            if (subscriber != null) {
//                subscriber.stopAsync().awaitTerminated();
//            }
//        }

    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {

        try {
            if (subscriptionAdminClient != null) {
                subscriptionAdminClient.close();
            }
            if (subscriber != null) {
                subscriber.stopAsync().awaitTerminated();
            }

        } catch (Exception e) {
            log.error("Error disconnecting the receiver", e);
        }

    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}
     */
    @Override
    public void destroy() {

    }

    /**
     * Called to pause event consumption
     */
    @Override
    public void pause() {
   messageProcessor.pause();
    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {
    messageProcessor.resume();
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as a map
     */
    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param map the stateful objects of the processing element as a map.
     * This map will have the  same keys that is created upon calling currentState() method.
     */
     @Override
     public void restoreState(Map<String, Object> map) {

     }
}

