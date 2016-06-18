package org.logstash;


import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MemoryQueuePageTest {

    private static byte[] BYTES_16 = "abcdefghijklmnop".getBytes();

    @Test
    public void testSimpleReadWrite() throws Exception {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        int head = qp.write(BYTES_16);
        assertEquals(BYTES_16.length + MemoryQueuePage.OVERHEAD_BYTES, head);
        assertEquals(1, qp.unused());
        List<AckedQueueItem> items = qp.read(2);
        assertEquals(1, items.size());
        assertArrayEquals(BYTES_16, items.get(0).getData());
        assertEquals(0, qp.unused());
    }

}

