package org.wso2.extension.siddhi.io.googlepubsub.sink.exception;

/**
 * Encapsulates the sink adapter exception details.
 */

public class GooglePubSubInputAdaptorRuntimeException extends RuntimeException {
    public GooglePubSubInputAdaptorRuntimeException() {
    }

    public GooglePubSubInputAdaptorRuntimeException(String message) {
        super(message);
    }


    public GooglePubSubInputAdaptorRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }


    public GooglePubSubInputAdaptorRuntimeException(Throwable cause) {
        super(cause);
    }
}
