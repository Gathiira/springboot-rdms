package com.jg.rdms.db.sql;

import java.util.Map;

public record UpdateCommand(
        String table,
        Map<String, Object> setValues,
        Map<String, Object> whereEquals
) {}