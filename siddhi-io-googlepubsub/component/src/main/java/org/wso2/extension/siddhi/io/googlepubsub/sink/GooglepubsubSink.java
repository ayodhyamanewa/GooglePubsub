package org.wso2.extension.siddhi.io.googlepubsub.sink;

import com.google.api.core.ApiFuture;;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.googlepubsub.sink.exception.GooglePubSubInputAdaptorRuntimeException;
import org.wso2.extension.siddhi.io.googlepubsub.util.GooglePubSubConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 */

@Extension(
        name = "googlepubsub",
        namespace = "sink",
        description = "The Google Pub Sub sink publishes the events into a Google Pub Sub processed by WSO2 Stream " +
                "Processor. If a topic doesn't exist in the server, Google Pub Sub sink creates a topic.",
        parameters = {
                @Parameter(
                        name = GooglePubSubConstants.GOOGLEPUBSUB_SERVER_PROJECTID,
                        description = "The unique ID of the project within which the topic is created.",
                        type = DataType.STRING,
                        dynamic = true),

                @Parameter(
                        name = GooglePubSubConstants.TOPIC_ID,
                        description = "The topic id to which events should be published. " +
                                "Only one topic must be specified.",
                        type =  DataType.STRING,
                        dynamic = true),


                @Parameter(
                        name = GooglePubSubConstants.GOOGLEPUBSUB_IDLETIMEOUT,
                        description = "Idle timeout of the connection.",
                        type =  DataType.INT,
                        dynamic = true,
                        optional = true, defaultValue = "50"),



        },
        examples = {
                @Example(description = "This example shows how to publish messages to a topic in the " +
                        "Google Pub Sub server with all supportive configurations.Accordingly, " +
                        "the messages are published to a topic named topicA in the project having a project id " +
                        " 'sp-path-1547649404768'.If the required topic already exists in the particular project " +
                        "the messages are directly published to that topic.If the required topic does not exist a" +
                        " new topic is created to the required topic id and then the messages are published to " +
                        "the particular topic.",
                        syntax = "@sink(type = 'googlepubsub', @map(type= 'text'), "
                                + "projectid = 'sp-path-1547649404768', "
                                + "topicid ='topicA', "
                                + ")\n" +
                                "define stream inputStream (message string);"),


        }
)

// for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sinks

public class GooglepubsubSink extends Sink {
    private static final Logger log = Logger.getLogger(GooglepubsubSink.class);

    private StreamDefinition streamDefinition;;
    private OptionHolder optionHolder;
    private String projectid;
    private String topicid;
    private int idletimeout;
    private ConfigReader configReader;
    private SiddhiAppContext siddhiAppContext;
    private String topic;
    private GoogleCredentials credentials;
    private TopicAdminClient topicAdminClient;
    private Publisher publisher;

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a sink of type file can only write objects of type String .
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class };
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{GooglePubSubConstants.TOPIC_ID, GooglePubSubConstants.GOOGLEPUBSUB_SERVER_PROJECTID };
    }

    /**
     * The initialization method for {@link Sink}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     * @param streamDefinition  containing stream definition bind to the {@link Sink}
     * @param optionHolder            Option holder containing static and dynamic configuration related
     *                                to the {@link Sink}
     * @param configReader        to read the sink related system configuration.
     * @param siddhiAppContext        the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to
     *                                get siddhi related utility functions.
     */
    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
            SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.optionHolder = optionHolder;
        this.configReader = configReader;
        this.siddhiAppContext = siddhiAppContext;
        this.topicid = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.TOPIC_ID);
        this.projectid = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.GOOGLEPUBSUB_SERVER_PROJECTID);

    }

    /**
     * This method will be called when events need to be published via this sink
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {

        String message = (String) payload;
        ProjectTopicName topic = ProjectTopicName.of(projectid, topicid);
        try {
            TopicAdminSettings topicAdminSettings =
                    TopicAdminSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();
            TopicAdminClient topicAdminClient =
                    TopicAdminClient.create(topicAdminSettings);
            topicAdminClient.createTopic(topic);
            log.info("A new topic created : " + topic);
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() != StatusCode.Code.ALREADY_EXISTS) {
                log.error("Error connecting to the topic in google pub sub server.");
                throw new ConnectionUnavailableException("Error in connecting to the topic in google pub subs server");
            }
        } catch (IOException e) {
            log.error("Could not create your required  topic: " + topic);
        }

        Publisher publisher = null;
        List<ApiFuture<String>> futures = new ArrayList<>();

        try {
            publisher = Publisher.newBuilder(topic).setCredentialsProvider
                    (FixedCredentialsProvider.create(credentials)).build();

            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();

            ApiFuture<String> future = publisher.publish(pubsubMessage);
            futures.add(future);

        } catch (IOException e) {
            log.error("Error creating a publisher bound to the topic : " + topic);
            throw new GooglePubSubInputAdaptorRuntimeException("Error creating a publisher bound to the topic : " +
                    topic, e);
        }
//        finally {
//            // Wait on any pending requests
//            List<String> messageIds = null;
//            try {
//                messageIds = ApiFutures.allAsList(futures).get();
//            } catch (InterruptedException e) {
//                log.info("");
//            } catch (ExecutionException e) {
//                log.info("");
//            }
//
//            for (String messageId : messageIds) {
//                log.info("The message is successfully published under id :" + messageId);
//            }
//
//
//        }
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        File credentialsPath = new File("/home/ayodhya/Desktop/Google/siddhi-io-googlepubsub/component/" +
                "src/main/resources/sp.json");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (FileNotFoundException e) {
            log.error("The file that contains your service account credentials is not found.");
        } catch (IOException e) {
            log.error("Error connecting to the Google Pub Sub server.");
            throw new ConnectionUnavailableException("Error connecting to the Google Pub Sub server : ", e);
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override
    public void disconnect() {
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
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    public void destroy() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time
     * This is also used to identify the internal states and debugging
     * @return all internal states should be return as an map with meaning full keys
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
     *              This map will have the  same keys that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> map) {

    }
}

