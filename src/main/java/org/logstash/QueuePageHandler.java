package org.logstash;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueuePageHandler implements Closeable {

    private final static List<AckedQueueItem> EMPTY_RESULT = new ArrayList<>(0);

    // write at the queue head
    // @return ?
    int write(byte[] data) {
        // TBD
    }

    // non-blocking read up to next n unusued item and mark them as in-use. if less than n items are available
    // these will be read and returned immediately.
    // @return List of read AckedQueueItem, or empty list if no items are read
    List<AckedQueueItem> read(int n) {
        // TBD
        return EMPTY_RESULT;
    }

    // blocking timed-out read of next n unusued item and mark them as in-use. if less than n items are available
    // this call will block and wait up to timeout ms and return an empty list if n items were not available.
    // @return List of read AckedQueueItem, or empty list if timeout is reached
    List<AckedQueueItem> read(int n, int timeout) {
        // TBD
        return EMPTY_RESULT;
    }

    // mark a list of AckedQueueItem as acknoleged
    void ack(List<AckedQueueItem> items) {
        // TBD
    }


    @Override
    public void close() throws IOException {
        // TBD
    }
}
