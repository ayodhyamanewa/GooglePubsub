package org.wso2.extension.siddhi.io.googlepubsub.sink;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.googlepubsub.source.PubSubMessageReceiver;
import org.wso2.extension.siddhi.io.googlepubsub.util.ResultContainer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * A test client to check the google pub sub publishing tasks.
 */
public class TestClient {
    private static final Logger log = Logger.getLogger(TestClient.class);
    private ResultContainer resultContainer;
    private boolean eventArrived;
    private int count;
    private Subscriber subscriber;
    private String projectid;
    private String topicid;
    private String subscriptionid;
    private TestClient publisher;

    public TestClient(String projectid, String topicid, String subscriptionid, ResultContainer resultContainer) {
        this.projectid = projectid;
        this.topicid = topicid;
        this.subscriptionid = subscriptionid;
        this.resultContainer = resultContainer;
    }


    public TestClient() {
        count = 0;
        eventArrived = false;

    }

    public void shutdown() {
        if (subscriber != null) {
            subscriber.stopAsync().awaitTerminated();
        }
        if (publisher != null) {
            publisher.shutdown();
        }
    }


    public void consumer() throws Exception {
        GoogleCredentials credentials = null;
        File credentialsPath = new File("/home/ayodhya/Desktop/Google/siddhi-io-googlepubsub/component/" +
                "src/main/resources/sp.json");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (FileNotFoundException e) {
            log.error("The file that points to your service account credentials is not found.");
        } catch (IOException e) {
            log.error("Credentials are missing.");
        }

        ProjectTopicName topicName = ProjectTopicName.of(projectid, topicid);

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(
                projectid, subscriptionid);
        try {
            SubscriptionAdminSettings subscriptionAdminSettings =
                    SubscriptionAdminSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();
            SubscriptionAdminClient subscriptionAdminClient =
                    SubscriptionAdminClient.create(subscriptionAdminSettings);

            Subscription subscription =
                    subscriptionAdminClient.createSubscription(
                            subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() != StatusCode.Code.ALREADY_EXISTS) {
                log.error("Check whether you have subscribed for the required topic.");
            }

        } catch (IOException e) {
            log.error("Could not create a subscription for your topic.");
        }


        Subscriber subscriber = null;
        PubSubMessageReceiver receiver = new PubSubMessageReceiver();
        subscriber =
                Subscriber.newBuilder(subscriptionName, receiver)
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
        subscriber.startAsync().awaitRunning();


        class Server implements Runnable {

            Subscriber subscriber = null;
            GoogleCredentials credentials;

            public Server(Subscriber subscriber, GoogleCredentials credentials1) {
                this.subscriber = subscriber;
                this.credentials = credentials1;
            }

            @Override
            public void run() {

                while (true) {
                    PubsubMessage message = null;
                    try {
                        message = receiver.getMessages().take();
                        //log.info("The message is :" + message.getData().toStringUtf8());
                        resultContainer.eventReceived(message.getData().toStringUtf8());
                        count++;
                        eventArrived = true;

                    } catch (InterruptedException e) {
                        log.error("Error receiving your message.");
                    }

                }
            }
        }

        Thread t = new Thread(new Server(subscriber, credentials));
        t.start();

    }


    public int getCount() {
        return count;
    }

    public boolean getEventArrived() {
        return eventArrived;
    }


}


