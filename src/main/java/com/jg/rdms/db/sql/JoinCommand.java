package com.jg.rdms.db.sql;

import java.util.Map;

public record JoinCommand(
        JoinType type,
        String leftTable,
        String rightTable,
        String leftColumn,
        String rightColumn,
        Map<String, Object> whereEquals
) {}
