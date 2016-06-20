package org.logstash;

import java.io.Closeable;
import java.util.List;

public interface QueuePage extends Closeable {

    // write at the page head
    // @return then new head position or 0 if not enough space left for data
    int write(byte[] data);

    // @param bytes the number of bytes for the payload excluding any metadata overhead
    // @return true if the number of bytes is writable in this queue page
    boolean writable(int bytes);

    // non-blocking read up to next n unusued item and mark them as in-use. if less than n items are available
    // this will be read and returned immediately.
    // @return List of read AckedQueueItem, or empty list if no items are read
    List<AckedQueueItem> read(int n);

    // blocking timed-out read of next n unusued item and mark them as in-use. if less than n items are available
    // this call will block and wait up to timeout ms and return an empty list if n items were not available.
    // @return List of read AckedQueueItem, or empty list if timeout is reached
    List<AckedQueueItem> read(int n, int timeout);

    // mark a list of AckedQueueItem as acknoleged
    void ack(List<AckedQueueItem> items);

    // mark a single item position offset as acknoledged
    void ack(int offset);

    // @return the number of unsued items
    int unused();

    // @return the page capacity in bytes
    int capacity();

    QueuePage setHead(int offset);
}
