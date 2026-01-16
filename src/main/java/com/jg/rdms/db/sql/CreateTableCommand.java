package com.jg.rdms.db.sql;


import com.jg.rdms.db.core.Column;

import java.util.List;

public record CreateTableCommand(String tableName, List<Column> columns) {
}
