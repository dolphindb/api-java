package com.xxdb.streaming.client.streamingSQL;

import java.util.EventListener;

/**
 * Listener for streaming SQL subscription events with concise naming.
 */
public interface UpdateListener extends EventListener {

    /**
     * Called when a batch of streaming SQL messages has been applied.
     */
    void onUpdate(UpdateEvent event);

    /**
     * Called when an exception occurs during subscription processing.
     */
    default void onError(String queryId, Throwable error) {}

    /**
     * Called when the subscription loop ends (client closed or unsubscribed).
     */
    default void onClose(String queryId) {}
}
