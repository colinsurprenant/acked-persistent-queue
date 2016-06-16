package org.logstash;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class AckedQueue implements Closeable {

    /**
     * Adds an item at the queue tail
     *
     * @param data to be pushed data
     * @throws IOException if any IO error in push operation
     */
    public void push(byte[] data) throws IOException
    {

    }

    public AckedQueueItem use() throws IOException
    {
        return null;
    }

    public List<AckedQueueItem> use(int batchSize) throws IOException
    {
        return null;
    }

    public void ack(AckedQueueItem)
    {

    }

    public void ack(List<AckedQueueItem>)
    {

    }

    public long size()
    {
        return 0L;
    }

    public void purge() throws IOException
    {

    }

    public void clear() throws IOException
    {

    }

    @Override
    public void close() throws IOException {

    }
}
