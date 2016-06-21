package org.logstash.ackedqueue;


import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PageHandler implements Closeable {

    private final static List<Element> EMPTY_RESULT = new ArrayList<>(0);

    private String dirPath;
    private int pageSize;
    private Metadata meta;
    private Map<Long, Page> livePages;

    // @param dirPath directory path where all queue data files will be written
    // @param pageSize the pageSize when creating a new queue, if the queue already exists, its configured page size will be used
    public PageHandler(String dirPath, int pageSize) {
        this.dirPath = dirPath;
        this.pageSize = pageSize;
        this.meta = null;
        this.livePages = new HashMap<>();
    }

    public void open() throws FileNotFoundException {
        Path p = FileSystems.getDefault().getPath(this.dirPath);

        if (Files.notExists(p, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileNotFoundException(this.dirPath);
        }

        // TODO: ajust when meta will be persisted & retrieved
        this.meta = new Metadata();
        this.meta.setPageSize(this.pageSize);
        this.meta.setHeadPageIndex(0);
        this.meta.setHeadPageOffset(0);
        this.meta.setUnackedTailPageIndex(0);
        this.meta.setUnusedTailPageIndex(0);

        this.livePages = new HashMap<>();
    }


    // write at the queue head
    // @return ?
    int write(byte[] data) {
        // TODO: check for data bigger that page capacity exception prior to any per page availibility attempt

        long headPageIndex = this.meta.getHeadPageIndex();

        // grab the head page, if there is not enough space left to write our data, just create a new head page
        Page headPage = page(headPageIndex);

        if (!headPage.writable(data.length)) {
            // just increment head page since we know the head is the last page and record new head index in metadata
            headPageIndex++;
            this.meta.setHeadPageIndex(headPageIndex);
            headPage = page(headPageIndex);
        }

        headPage.write(data);

        // record the new head page offset in metadata
        // TODO: do we really need to track the head page offset?
        this.meta.setHeadPageOffset(headPage.getHead());

        return 0;
    }

    // non-blocking read up to next n unusued item and mark them as in-use. if less than n items are available
    // these will be read and returned immediately.
    // @return List of read Element, or empty list if no items are read
    List<Element> read(int n) {
        long unusedTail = this.meta.getUnusedTailPageIndex();

        int remaining = n;
        List<Element> result = new ArrayList<>();

        for (;;) {
            Page p = page(unusedTail);
            result.addAll(p.read(remaining));
            remaining = n - result.size();

            if (remaining <= 0 || lastPage(unusedTail)) {
                return result;
            }

            unusedTail++;
            this.meta.setUnusedTailPageIndex(unusedTail);
        }
    }

    // blocking timed-out read of next n unusued item and mark them as in-use. if less than n items are available
    // this call will block and wait up to timeout ms and return an empty list if n items were not available.
    // @return List of read Element, or empty list if timeout is reached
    List<Element> read(int n, int timeout) {
        // TBD
        return EMPTY_RESULT;
    }

    // mark a list of Element as acknoleged
    void ack(List<Element> items) {
        // TBD
    }


    @Override
    public void close() throws IOException {
        // TBD
    }

    // @param index the page index to retrieve
    private Page page(long index) {
        // TODO: adjust implementation for correct live pages handling
        // TODO: extract page caching in a separate class?

        Page p = this.livePages.get(index);
        if (p != null) {
            return p;
        }

        p = new MemoryPage(this.pageSize, index);
        this.livePages.put(index, p);
        return p;
    }

    private boolean lastPage(long index) {
        return index >= this.meta.getHeadPageIndex();
    }
}
