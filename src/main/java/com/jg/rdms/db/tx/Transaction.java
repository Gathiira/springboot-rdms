package com.jg.rdms.db.tx;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Transaction {
    public final long id;
    public boolean committed = false;

    public Transaction(long id) {
        this.id = id;
    }
}
