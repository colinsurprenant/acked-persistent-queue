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

    // read next n unusued item and mark them as in-use
    // @return List of read AckedQueueItem
    List<AckedQueueItem> read(int n);

    // mark a list of AckedQueueItem as acknoleged
    void ack(List<AckedQueueItem> items);

    // mark a single item position as acknoledged
    void ack(int position);

    // @return the number of unsued items
    int unused();

    // @return the page capacity in bytes
    int capacity();

    QueuePage setHead(int position);

    QueuePage setTail(int position);
}
