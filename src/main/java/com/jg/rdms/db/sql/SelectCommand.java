package com.jg.rdms.db.sql;

import java.util.Map;

public record SelectCommand(
        String table,
        Map<String, Object> whereEquals
) {}
