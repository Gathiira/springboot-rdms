package com.jg.rdms.db.tx;

import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private final AtomicLong counter = new AtomicLong();

    public Transaction begin() {
        return new Transaction(counter.incrementAndGet());
    }

    public void commit(Transaction tx) {
        tx.committed = true;
    }
}
