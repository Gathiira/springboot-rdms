package com.jg.rdms.db.core;


import java.util.concurrent.atomic.AtomicInteger;

public class IdGenerator {
    private final AtomicInteger counter = new AtomicInteger(0);

    public int nextId() {
        return counter.incrementAndGet();
    }

    public void set(int value) {
        counter.set(value);
    }
}
