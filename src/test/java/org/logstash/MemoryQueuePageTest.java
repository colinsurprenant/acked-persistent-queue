package org.logstash;


import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MemoryQueuePageTest {

    private static byte[] A_BYTES_16 = "aaaaaaaaaaaaaaaa".getBytes();
    private static byte[] B_BYTES_16 = "bbbbbbbbbbbbbbbb".getBytes();

    // without acks

    @Test
    public void testSingleWriteRead() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        int head = qp.write(A_BYTES_16);
        assertEquals(A_BYTES_16.length + MemoryQueuePage.OVERHEAD_BYTES, head);
        assertEquals(1, qp.unused());
        List<AckedQueueItem> items = qp.read(2);
        assertEquals(1, items.size());
        assertArrayEquals(A_BYTES_16, items.get(0).getData());
        assertEquals(0, qp.unused());
    }

    @Test
    public void testOverflow() {
        MemoryQueuePage qp = new MemoryQueuePage(15);
        assertFalse(qp.writable(16));
        assertEquals(0, qp.write(A_BYTES_16));

        assertTrue(qp.writable(15 - MemoryQueuePage.OVERHEAD_BYTES));
        assertFalse(qp.writable(15 - MemoryQueuePage.OVERHEAD_BYTES + 1));
    }

    @Test
    public void testDoubleWriteRead() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        int head = qp.write(A_BYTES_16);
        assertEquals(A_BYTES_16.length + MemoryQueuePage.OVERHEAD_BYTES, head);
        assertEquals(1, qp.unused());

        head = qp.write(B_BYTES_16);
        assertEquals((B_BYTES_16.length + MemoryQueuePage.OVERHEAD_BYTES) * 2, head);
        assertEquals(2, qp.unused());

        List<AckedQueueItem> items = qp.read(2);
        assertEquals(0, qp.unused());

        assertEquals(2, items.size());
        assertArrayEquals(A_BYTES_16, items.get(0).getData());
        assertArrayEquals(B_BYTES_16, items.get(1).getData());
    }

    @Test
    public void testDoubleWriteSingleRead() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        int head = qp.write(A_BYTES_16);
        assertEquals(A_BYTES_16.length + MemoryQueuePage.OVERHEAD_BYTES, head);
        assertEquals(1, qp.unused());

        head = qp.write(B_BYTES_16);
        assertEquals((B_BYTES_16.length + MemoryQueuePage.OVERHEAD_BYTES) * 2, head);
        assertEquals(2, qp.unused());

        List<AckedQueueItem> items = qp.read(1);
        assertEquals(1, qp.unused());

        assertEquals(1, items.size());
        assertArrayEquals(A_BYTES_16, items.get(0).getData());

        items = qp.read(1);
        assertEquals(0, qp.unused());

        assertEquals(1, items.size());
        assertArrayEquals(B_BYTES_16, items.get(0).getData());
    }

    @Test
    public void testLargerRead() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        qp.write(A_BYTES_16);
        qp.write(B_BYTES_16);
        assertEquals(2, qp.unused());

        List<AckedQueueItem> items = qp.read(3);
        assertEquals(0, qp.unused());
        assertEquals(2, items.size());
    }

    @Test
    public void testEmptyRead() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        qp.write(A_BYTES_16);
        qp.write(B_BYTES_16);
        assertEquals(2, qp.unused());

        List<AckedQueueItem> items = qp.read(2);
        assertEquals(0, qp.unused());
        assertEquals(2, items.size());

        items = qp.read(2);
        assertEquals(0, qp.unused());
        assertEquals(0, items.size());
    }

    @Test
    public void testWriteReadReset() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        qp.write(A_BYTES_16);
        qp.write(B_BYTES_16);

        List<AckedQueueItem> items = qp.read(1);
        assertEquals(1, items.size());
        items = qp.read(1);
        assertEquals(1, items.size());

        assertEquals(0, qp.unused());

        qp.resetUnused();

        // all items are now maked as unused, we should be able to re-read all items
        assertEquals(2, qp.unused());

        items = qp.read(1);
        assertEquals(1, items.size());
        items = qp.read(1);
        assertEquals(1, items.size());

        assertEquals(0, qp.unused());
    }

    // with acks

    @Test
    public void testWriteReadAckReset() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        qp.write(A_BYTES_16);
        qp.write(B_BYTES_16);

        List<AckedQueueItem> items = qp.read(1);
        assertEquals(1, items.size());
        qp.ack(items);

        items = qp.read(1);
        assertEquals(1, items.size());
        qp.ack(items);

        assertEquals(0, qp.unused());

        qp.resetUnused();

        assertEquals(0, qp.unused());
    }

    @Test
    public void testWriteReadPartialAckReset() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        qp.write(A_BYTES_16);
        qp.write(B_BYTES_16);

        List<AckedQueueItem> items = qp.read(1);
        assertEquals(1, items.size());

        assertEquals(1, qp.unused());

        qp.ack(items);

        assertEquals(1, qp.unused());

        qp.resetUnused();

        assertEquals(1, qp.unused());
    }

    @Test
    public void testWritePartialAckReset() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        qp.write(A_BYTES_16);
        qp.write(B_BYTES_16);

        List<AckedQueueItem> items = qp.read(1);
        qp.resetUnused();

        assertEquals(2, qp.unused());

        qp.ack(items);

        assertEquals(1, qp.unused());
    }

    @Test
    public void testWritePartialAckRead() {
        MemoryQueuePage qp = new MemoryQueuePage(1024);
        qp.write(A_BYTES_16);
        qp.write(B_BYTES_16);

        List<AckedQueueItem> items = qp.read(1);
        qp.resetUnused();

        assertEquals(2, qp.unused());

        qp.ack(items);

        items = qp.read(2);
        assertEquals(1, items.size());
    }
}

