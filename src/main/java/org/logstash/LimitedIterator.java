package org.logstash;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LimitedIterator<T> implements Iterator<T> {
    private Iterator<T> wrapped;
    private int limit;
    private int iterated;

    public LimitedIterator(Iterator<T> wrapped, int limit) {
        this.wrapped = wrapped;
        this.limit = limit;
    }


    public boolean hasNext() {
        if (iterated < limit) {
            return wrapped.hasNext();
        }
        return false;
    }

    public T next() {
        if (iterated >= limit) {
            throw new NoSuchElementException();
        }

        iterated++;
        return wrapped.next();
    }
}