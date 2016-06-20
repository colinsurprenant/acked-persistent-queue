package org.logstash;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MemoryQueuePage implements QueuePage {
    private final static int INT_BYTE_SIZE = Integer.SIZE / Byte.SIZE;
    public final static int OVERHEAD_BYTES = INT_BYTE_SIZE + INT_BYTE_SIZE;

    private final static List<AckedQueueItem> EMPTY_RESULT = new ArrayList<>(0);

    private ByteBuffer data;
    private int capacity;
    private int head;
    private int tail;

    private RoaringBitmap unused;
    private RoaringBitmap unacked;

    // @param capacity page byte size
    public MemoryQueuePage(int capacity) {
        this.data = ByteBuffer.allocate(capacity);
        this.capacity = capacity;
        this.head = 0;
        this.tail = 0;
        this.unused = new RoaringBitmap();
        this.unacked = new RoaringBitmap();
    }

    // @return then new head position or 0 if not enough space left for data
    @Override
    public int write(byte[] data) {
        if (! writable(data.length)) {
            return 0;
        }

        this.data.position(this.head);

        // this write sequence total bytes must equate totalBytes(data.length)
        this.data.putInt(data.length);
        this.data.put(data);
        this.data.putInt(0);

        // set bitmaps
        this.unused.add(this.head);
        this.unacked.add(this.head);

        this.head += totalBytes(data.length);

        return this.head;
   }

    @Override
    public boolean writable(int bytes) {
        return (availableBytes() >= totalBytes(bytes));
    }

    @Override
    public List<AckedQueueItem> read(int n) {
        RoaringBitmap readable = readable();

        // empty result optimization
        if (readable.getCardinality() <= 0) {
            return EMPTY_RESULT;
        }

        List<AckedQueueItem> result = new ArrayList<>();

        Iterator i = new LimitedIterator(readable.iterator(), n);
        while (i.hasNext()) {
            int offset = (int) i.next();

            this.data.position(offset);

            int dataSize = this.data.getInt();
            assert dataSize > 0;

            byte[] payload = new byte[dataSize];;
            this.data.get(payload);

            // TODO: how/where should we track page index?
            result.add(new AckedQueueItem(payload, 0, offset));

            // set this item as in-use (unset unused bit)
            unused.remove(offset);
         }

        return result;
    }

    @Override
    public List<AckedQueueItem> read(int n, int timeout) {
        // TODO: TBD
        return EMPTY_RESULT;
    }

    @Override
    public void ack(List<AckedQueueItem> items) {
        items.forEach(item -> ack(item.pageOffet));
    }

    @Override
    public void ack(int offset) {
        this.unacked.remove(offset);
    }

    @Override
    public int unused() {
        return readable().getCardinality();
    }

    @Override
    public int capacity() {
        return this.capacity;
    }

    @Override
    public QueuePage setHead(int offset) {
        this.head = offset;
        return this;
    }

    @Override
    public QueuePage setTail(int offset) {
        this.tail = offset;
        return this;
    }

    @Override
    public void close() throws IOException {
        // TBD
    }

    public void resetUnused() {
        // reset unused bits to the state of the unacked bits
        this.unused = new RoaringBitmap(this.unacked.toMutableRoaringBitmap());
    }

    private int availableBytes() {
        return this.capacity - this.head;
    }

    private RoaringBitmap readable() {
        // select items that are both marked as unused and unacked
        return RoaringBitmap.and(this.unused, this.unacked);
    }

    private int totalBytes(int dataSize)
    {
        return OVERHEAD_BYTES + dataSize;
    }

    private int maxReadOffset() {
        return (this.head - (INT_BYTE_SIZE + 2));
    }
}

