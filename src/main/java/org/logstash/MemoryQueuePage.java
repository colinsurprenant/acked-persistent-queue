package org.logstash;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MemoryQueuePage implements QueuePage {
    private final static int INT_BYTE_SIZE = Integer.SIZE / Byte.SIZE;
    public final static int OVERHEAD_BYTES = INT_BYTE_SIZE + INT_BYTE_SIZE;

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

        // this write sequence total bytes must equate dataWithOverhead(data.length)
        this.data.putInt(data.length);
        this.data.put(data);
        this.data.putInt(0);

        // set bitmaps
        this.unused.add(this.head);
        this.unacked.add(this.head);

        this.head += dataWithOverhead(data.length);

        return this.head;
   }

    @Override
    public boolean writable(int bytes) {
        return (available() >= dataWithOverhead(bytes));
    }

    @Override
    public List<AckedQueueItem> read(int n) {
        List<AckedQueueItem> result = new ArrayList<>();

//        this.unused.forEach(new IntConsumer() {
//
//            @Override
//            public void accept(int i) {
//                data.position(i);
//
//                int dataSize = data.getInt();
//                assert dataSize > 0;
//
//                byte[] payload = new byte[dataSize];;
//                data.get(payload);
//
//                // TODO: how/where should we track page index?
//                result.add(new AckedQueueItem(payload, 0, i));
//
//                // set this item as in-use.
//                unused.remove(i);
//            }
//
//        });


        // select items that are both marked as unused and unacked
        RoaringBitmap selected = RoaringBitmap.and(this.unused, this.unacked);

        Iterator i = new LimitedIterator(selected.iterator(), n);
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
    public void ack(List<AckedQueueItem> items) {
        items.forEach(item -> ack(item.pageOffet));
    }

    @Override
    public void ack(int position) {
        this.unacked.remove(position);
    }

    @Override
    public int unused() {
        return this.unused.getCardinality();
    }

    @Override
    public int capacity() {
        return this.capacity;
    }

    @Override
    public QueuePage setHead(int position) {
        this.head = position;
        return this;
    }

    @Override
    public QueuePage setTail(int position) {
        this.tail = position;
        return this;
    }

    @Override
    public void close() throws IOException {
        // TBD
    }

    public void resetUnused() {
        // reset unacked bits to
        this.unused = new RoaringBitmap(this.unacked.toMutableRoaringBitmap());
    }

    private int available() {
        return this.capacity - this.head;
    }

    private int dataWithOverhead(int dataSize)
    {
        return OVERHEAD_BYTES + dataSize;
    }

    private int maxReadOffset() {
        return (this.head - (INT_BYTE_SIZE + 2));
    }
}

