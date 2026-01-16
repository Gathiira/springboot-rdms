package com.jg.rdms.db.sql;

import java.util.Map;

public record DeleteCommand(
        String table,
        Map<String, Object> whereEquals
) {}
