package org.logstash.ackedqueue;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.List;


public class PageHandlerTest {

    private static byte[] A_BYTES_16 = "aaaaaaaaaaaaaaaa".getBytes();
    private static byte[] B_BYTES_16 = "bbbbbbbbbbbbbbbb".getBytes();

    @Test(expected = FileNotFoundException.class)
    public void testOpenInvalidPath() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/gaga", 1024);
        ph.open();
    }

    @Test
    public void testOpenValidPath() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", 1024);
        ph.open();
    }


    @Test
    public void testSingleWriteRead() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", A_BYTES_16.length + Page.OVERHEAD_BYTES);
        ph.open();
        ph.write(A_BYTES_16);
        List<Element> result = ph.read(1);
        assertEquals(1 , result.size());
        assertArrayEquals(A_BYTES_16, result.get(0).getData());
        assertEquals(0, result.get(0).getPageIndex());
        assertEquals(0, result.get(0).getPageOffet());
    }

    @Test
    public void testSingleWriteBiggerRead() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", A_BYTES_16.length + Page.OVERHEAD_BYTES);
        ph.open();
        ph.write(A_BYTES_16);
        List<Element> result = ph.read(2);
        assertEquals(1 , result.size());
        assertArrayEquals(A_BYTES_16, result.get(0).getData());
        assertEquals(0, result.get(0).getPageIndex());
        assertEquals(0, result.get(0).getPageOffet());
    }

    @Test
    public void testSingleWriteDualRead() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", A_BYTES_16.length + Page.OVERHEAD_BYTES);
        ph.open();
        ph.write(A_BYTES_16);
        List<Element> result = ph.read(1);
        assertEquals(1 , result.size());
        result = ph.read(1);
        assertEquals(0, result.size());
    }

    @Test
    public void testMultipleWriteSingleRead() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", A_BYTES_16.length + Page.OVERHEAD_BYTES);
        ph.open();
        ph.write(A_BYTES_16);
        ph.write(B_BYTES_16);
        List<Element> result = ph.read(2);
        assertEquals(2 , result.size());

        assertArrayEquals(A_BYTES_16, result.get(0).getData());
        assertArrayEquals(B_BYTES_16, result.get(1).getData());

        result = ph.read(1);
        assertEquals(0, result.size());
    }

    @Test
    public void testMultipleWriteRead() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", A_BYTES_16.length + Page.OVERHEAD_BYTES);
        ph.open();
        ph.write(A_BYTES_16);
        ph.write(B_BYTES_16);
        List<Element> result = ph.read(1);
        assertEquals(1, result.size());
        assertArrayEquals(A_BYTES_16, result.get(0).getData());

        result = ph.read(1);
        assertEquals(1, result.size());
        assertArrayEquals(B_BYTES_16, result.get(0).getData());

        result = ph.read(1);
        assertEquals(0 , result.size());
    }

    @Test
    public void testMultipleWriteBiggerRead() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", A_BYTES_16.length + Page.OVERHEAD_BYTES);
        ph.open();
        ph.write(A_BYTES_16);
        ph.write(B_BYTES_16);
        List<Element> result = ph.read(3);
        assertEquals(2, result.size());
        assertArrayEquals(A_BYTES_16, result.get(0).getData());
        assertArrayEquals(B_BYTES_16, result.get(1).getData());

        result = ph.read(1);
        assertEquals(0 , result.size());
    }


    // acking

    @Test
    public void testSingleWriteReadAck() throws FileNotFoundException {
        PageHandler ph = new PageHandler("/tmp", A_BYTES_16.length + Page.OVERHEAD_BYTES);
        ph.open();
        ph.write(A_BYTES_16);
        List<Element> result = ph.read(1);
        assertEquals(1 , result.size());
        assertArrayEquals(A_BYTES_16, result.get(0).getData());
        assertEquals(0, result.get(0).getPageIndex());
        assertEquals(0, result.get(0).getPageOffet());
    }

}

