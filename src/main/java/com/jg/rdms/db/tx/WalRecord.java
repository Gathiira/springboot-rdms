package com.jg.rdms.db.tx;

import java.io.Serializable;
import java.util.Map;

public record WalRecord(
        long txId,
        String table,
        Map<String, Object> row
) implements Serializable {}