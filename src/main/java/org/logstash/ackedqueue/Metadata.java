package org.logstash.ackedqueue;


import java.io.Closeable;
import java.io.IOException;

public class Metadata implements Closeable {
    // head tracking for writes
    private long headPageIndex;
    private int headPageOffset;

    // tail tracking for reads. offset tracking is not necessary since it uses the per-page bitsets
    private long tailPageIndex;

    public long getHeadPageIndex() {
        return headPageIndex;
    }

    public void setHeadPageIndex(long headPageIndex) {
        this.headPageIndex = headPageIndex;
    }

    public int getHeadPageOffset() {
        return headPageOffset;
    }

    public void setHeadPageOffset(int headPageOffset) {
        this.headPageOffset = headPageOffset;
    }

    public long getTailPageIndex() {
        return tailPageIndex;
    }

    public void setTailPageIndex(long tailPageIndex) {
        this.tailPageIndex = tailPageIndex;
    }

    @Override
    public void close() throws IOException {
        // TBD
    }
}
