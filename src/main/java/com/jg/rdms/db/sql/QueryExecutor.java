package com.jg.rdms.db.sql;

import com.jg.rdms.db.core.Column;
import com.jg.rdms.db.core.Database;
import com.jg.rdms.db.core.PersistentTable;
import com.jg.rdms.db.core.Table;
import com.jg.rdms.db.tx.Transaction;
import com.jg.rdms.db.tx.TransactionManager;

import java.util.*;

public class QueryExecutor {

    private final Database db;
    private final TransactionManager txManager;

    public QueryExecutor(Database db, TransactionManager txManager) {
        this.db = db;
        this.txManager = txManager;
    }

    public Object execute(String sql) {
        sql = sql.trim();
        String upper = sql.toUpperCase();

        if (upper.startsWith("CREATE TABLE")) {
            executeCreateTable(
                    SqlParser.parseCreateTable(sql)
            );
            return "OK";
        }

        if (upper.contains(" JOIN ")) {
            return executeJoin(SqlParser.parseJoin(sql));
        }

        if (upper.startsWith("INSERT")) {
            return executeInsert(SqlParser.parseInsert(sql));
        }

        if (sql.startsWith("UPDATE")) {
            return executeUpdate(
                    SqlParser.parseUpdate(sql)
            );
        }

        if (upper.startsWith("SELECT")) {
            return executeSelect(
                    SqlParser.parseSelect(sql)
            );
        }

        if (upper.startsWith("DELETE")) {
            return executeDelete(
                    SqlParser.parseDelete(sql)
            );
        }

        throw new UnsupportedOperationException(
                "Unsupported SQL: " + sql
        );
    }

    /* -------------------------
       UPDATE execution
       ------------------------- */

    private int executeUpdate(UpdateCommand cmd) {

        PersistentTable table =
                (PersistentTable) db.table(cmd.table());

        Transaction tx = txManager.begin();

        try {
            int updated = 0;

            // Multi-condition WHERE implemented here
            for (Map<String, Object> row : table.selectAll()) {

                if (!matches(row, cmd.whereEquals())) {
                    continue;
                }

                updated += table.updateWhere(
                        tx,
                        "id",                // anchor update by PK
                        row.get("id"),
                        cmd.setValues()
                );
            }

            txManager.commit(tx);
            return updated;

        } catch (Exception e) {
            // rollback will be added later
            throw e;
        }
    }

    private boolean matches(
            Map<String, Object> row,
            Map<String, Object> conditions
    ) {
        for (var entry : conditions.entrySet()) {
            if (!row.containsKey(entry.getKey())) {
                return false;
            }
            if (!row.get(entry.getKey()).equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private int executeDelete(DeleteCommand cmd) {

        PersistentTable table =
                (PersistentTable) db.table(cmd.table());

        Transaction tx = txManager.begin();

        try {
            int deleted = 0;

            // Copy to avoid concurrent modification
            for (var row : List.copyOf(table.selectAll())) {

                if (!matches(row, cmd.whereEquals())) {
                    continue;
                }

                table.deleteWhere(
                        "id",
                        row.get("id")
                );

                deleted++;
            }

            txManager.commit(tx);
            return deleted;

        } catch (Exception e) {
            // rollback later
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> executeSelect(SelectCommand cmd) {

        Table table = db.table(cmd.table());

        if (table == null) {
            throw new IllegalArgumentException(
                    "Table does not exist: " + cmd.table()
            );
        }

        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> row : table.selectAll()) {

            if (!matches(row, cmd.whereEquals())) {
                continue;
            }

            // defensive copy
            result.add(new HashMap<>(row));
        }

        return result;
    }

    private int executeInsert(InsertCommand cmd) {

        Table table = db.table(cmd.table());

        if (table == null) {
            throw new IllegalArgumentException(
                    "Table does not exist: " + cmd.table()
            );
        }

        Map<String, Object> row = new HashMap<>();

        for (int i = 0; i < cmd.columns().size(); i++) {
            row.put(
                    cmd.columns().get(i),
                    cmd.values().get(i)
            );
        }

        Transaction tx = txManager.begin();

        try {
            table.insert(tx, row);
            txManager.commit(tx);
            return 1;
        } catch (Exception e) {
            // rollback later
            throw e;
        }
    }

    private List<Map<String, Object>> executeJoin(JoinCommand cmd) {

        Table left = db.table(cmd.leftTable());
        Table right = db.table(cmd.rightTable());

        // Try index-based join first
        Optional<List<Map<String, Object>>> indexed;

        indexed = right.lookupByColumn(cmd.rightColumn(), null);

        if (indexed.isPresent()) {
            return executeIndexJoin(cmd, left, right);
        }

        if (left == null) {
            throw new IllegalArgumentException("Table not found");
        }

        // Choose smaller table as build side
        boolean buildLeft = left.selectAll().size() <= right.selectAll().size();

        Table buildTable = buildLeft ? left : right;
        Table probeTable = buildLeft ? right : left;

        String buildColumn = buildLeft ? cmd.leftColumn() : cmd.rightColumn();
        String probeColumn = buildLeft ? cmd.rightColumn() : cmd.leftColumn();

        String buildName = buildLeft ? cmd.leftTable() : cmd.rightTable();
        String probeName = buildLeft ? cmd.rightTable() : cmd.leftTable();

        // 1️⃣ Build phase
        Map<Object, List<Map<String, Object>>> hashTable = new HashMap<>();

        for (Map<String, Object> row : buildTable.selectAll()) {
            Object key = row.get(buildColumn);
            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        // 2️⃣ Probe phase
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> probeRow : probeTable.selectAll()) {

            Object key = probeRow.get(probeColumn);
            List<Map<String, Object>> matches = hashTable.get(key);

            if (matches == null) {
                // LEFT / RIGHT join handling
                if ((cmd.type() == JoinType.LEFT && !buildLeft) ||
                        (cmd.type() == JoinType.RIGHT && buildLeft)) {

                    result.add(joinWithNulls(
                            probeName,
                            probeRow,
                            buildName,
                            buildTable
                    ));
                }
                continue;
            }

            for (Map<String, Object> buildRow : matches) {
                Map<String, Object> joined =
                        buildLeft
                                ? joinRows(buildName, buildRow, probeName, probeRow)
                                : joinRows(probeName, probeRow, buildName, buildRow);

                if (matches(joined, cmd.whereEquals())) {
                    result.add(joined);
                }
            }
        }

        return result;
    }

    private Map<String, Object> joinRows(
            String leftName,
            Map<String, Object> left,
            String rightName,
            Map<String, Object> right
    ) {
        Map<String, Object> row = new HashMap<>();

        for (var e : left.entrySet()) {
            row.put(leftName + "." + e.getKey(), e.getValue());
        }
        for (var e : right.entrySet()) {
            row.put(rightName + "." + e.getKey(), e.getValue());
        }

        return row;
    }

    private Map<String, Object> joinWithNulls(
            String presentTable,
            Map<String, Object> presentRow,
            String nullTable,
            Table nullTableRef
    ) {
        Map<String, Object> row = new HashMap<>();

        for (var e : presentRow.entrySet()) {
            row.put(presentTable + "." + e.getKey(), e.getValue());
        }

        for (Column c : nullTableRef.getColumns()) {
            row.put(nullTable + "." + c.name(), null);
        }

        return row;
    }

    private List<Map<String, Object>> executeIndexJoin(
            JoinCommand cmd,
            Table left,
            Table right
    ) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> lrow : left.selectAll()) {

            Object key = lrow.get(cmd.leftColumn());

            List<Map<String, Object>> matches =
                    right.lookupByColumn(cmd.rightColumn(), key)
                            .orElse(List.of());

            if (matches.isEmpty()) {
                if (cmd.type() == JoinType.LEFT) {
                    result.add(joinWithNulls(
                            cmd.leftTable(), lrow,
                            cmd.rightTable(), right
                    ));
                }
                continue;
            }

            for (Map<String, Object> rrow : matches) {

                Map<String, Object> joined =
                        joinRows(
                                cmd.leftTable(), lrow,
                                cmd.rightTable(), rrow
                        );

                if (matches(joined, cmd.whereEquals())) {
                    result.add(joined);
                }
            }
        }

        return result;
    }

    private void executeCreateTable(CreateTableCommand cmd) {
        db.execute(cmd);
    }
}
