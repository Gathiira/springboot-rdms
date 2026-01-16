package com.jg.rdms.db.core;

import com.jg.rdms.db.tx.Transaction;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@AllArgsConstructor
@Setter
@Getter
public class Table {
    protected final String name;
    protected final List<Column> columns;
    protected final List<Map<String, Object>> rows = new ArrayList<>();

    public void insert(Transaction tx, Map<String, Object> row) {
        throw new UnsupportedOperationException("Not persistent");
    }


    public List<Map<String, Object>> selectAll() {
        return rows;
    }

    public void deleteWhere(String column, Object value) {
        rows.removeIf(row -> value.equals(row.get(column)));
    }

    public Optional<List<Map<String, Object>>> lookupByColumn(
            String column,
            Object value
    ) {
        return Optional.empty();
    }

}
