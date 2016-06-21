package org.logstash.ackedqueue;


import java.io.Closeable;
import java.io.IOException;

public class Metadata implements Closeable {
    // head tracking for writes
    private long headPageIndex;
    private int headPageOffset;

    // tail tracking, offset tracking is not necessary since it uses the per-page bitsets
    private long unackedTailPageIndex; // tail page with the oldest unacked bits
    private long unusedTailPageIndex;  // tail page with the oldest unused bits

    // in use page byte size
    private int pageSize;

    public Metadata() {
        // TBD
    }

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

    public long getUnackedTailPageIndex() {
        return unackedTailPageIndex;
    }

    public void setUnackedTailPageIndex(long unackedTailPageIndex) {
        this.unackedTailPageIndex = unackedTailPageIndex;
    }

    public long getUnusedTailPageIndex() {
        return unusedTailPageIndex;
    }

    public void setUnusedTailPageIndex(long unusedTailPageIndex) {
        this.unusedTailPageIndex = unusedTailPageIndex;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public void close() throws IOException {
        // TBD
    }
}
