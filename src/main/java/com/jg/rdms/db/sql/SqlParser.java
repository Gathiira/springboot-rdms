package com.jg.rdms.db.sql;

import com.jg.rdms.db.core.Column;
import com.jg.rdms.db.core.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlParser {

    /* =========================
       ENTRY POINTS
       ========================= */

    public static CreateTableCommand parseCreateTable(String sql) {

        sql = normalize(sql);
        String upper = sql.toUpperCase();

        if (!upper.startsWith("CREATE TABLE")) {
            throw new IllegalArgumentException("Not a CREATE TABLE statement");
        }

        String tableName = extractTableName(sql);

        String columnBlock = extractColumnBlock(sql);

        List<Column> columns = parseColumns(columnBlock);

        return new CreateTableCommand(tableName, columns);
    }

    public static UpdateCommand parseUpdate(String sql) {

        // Normalize
        sql = sql.trim().replaceAll("\\s+", " ");

        // UPDATE users SET name='Bob' WHERE id=1 AND name='Jane'
        String upper = sql.toUpperCase();

        if (!upper.startsWith("UPDATE")) {
            throw new IllegalArgumentException("Not an UPDATE statement");
        }

        String table =
                sql.split(" ")[1];

        String setPart =
                sql.substring(
                        upper.indexOf("SET") + 3,
                        upper.contains("WHERE")
                                ? upper.indexOf("WHERE")
                                : sql.length()
                ).trim();

        String wherePart =
                upper.contains("WHERE")
                        ? sql.substring(upper.indexOf("WHERE") + 5).trim()
                        : "";

        Map<String, Object> setValues =
                parseAssignments(setPart);

        Map<String, Object> whereEquals =
                wherePart.isEmpty()
                        ? Map.of()
                        : parseConditions(wherePart);

        return new UpdateCommand(
                table,
                setValues,
                whereEquals
        );
    }

        /* -------------------------
       Helpers
       ------------------------- */

    private static Map<String, Object> parseAssignments(String input) {
        Map<String, Object> result = new HashMap<>();

        // name='Bob', age=10
        for (String part : input.split(",")) {
            String[] kv = part.split("=");
            result.put(
                    kv[0].trim(),
                    parseValue(kv[1].trim())
            );
        }
        return result;
    }

    private static Map<String, Object> parseConditions(String input) {
        Map<String, Object> result = new HashMap<>();

        // id=1 AND name='Jane'
        for (String part : input.split("AND")) {
            String[] kv = part.split("=");
            result.put(
                    kv[0].trim(),
                    parseValue(kv[1].trim())
            );
        }
        return result;
    }

    private static Object parseValue(String raw) {
        raw = raw.trim();

        if (raw.startsWith("'") && raw.endsWith("'")) {
            return raw.substring(1, raw.length() - 1);
        }

        return Integer.valueOf(raw);
    }

    /* =========================
       CREATE TABLE helpers
       ========================= */

    private static String extractTableName(String sql) {
        // CREATE TABLE users (...
        return sql.split(" ")[2];
    }

    private static String extractColumnBlock(String sql) {
        int start = sql.indexOf('(');
        int end = sql.lastIndexOf(')');

        if (start == -1 || end == -1 || end <= start) {
            throw new IllegalArgumentException(
                    "Invalid CREATE TABLE syntax: missing column definition block"
            );
        }

        return sql.substring(start + 1, end).trim();
    }
    private static List<Column> parseColumns(String block) {

        List<Column> columns = new ArrayList<>();

        for (String rawCol : block.split(",")) {

            String colDef = rawCol.trim();
            String upper = colDef.toUpperCase();

            String[] parts = colDef.split("\\s+");

            if (parts.length < 2) {
                throw new IllegalArgumentException(
                        "Invalid column definition: " + colDef
                );
            }

            String name = parts[0];
            DataType type = DataType.valueOf(parts[1]);

            boolean primary = upper.contains("PRIMARY KEY");
            boolean unique  = upper.contains("UNIQUE");

            String refTable = null;
            String refColumn = null;

            if (upper.contains("REFERENCES")) {
                int idx = upper.indexOf("REFERENCES") + "REFERENCES".length();
                String ref = colDef.substring(idx).trim();

                // users(id)
                int open = ref.indexOf('(');
                int close = ref.indexOf(')');

                refTable = ref.substring(0, open).trim();
                refColumn = ref.substring(open + 1, close).trim();
            }

            columns.add(new Column(
                    name,
                    type,
                    primary,
                    unique,
                    refTable,
                    refColumn
            ));
        }

        return columns;
    }

    /* =========================
       Shared helpers
       ========================= */

    private static String normalize(String sql) {
        return sql
                .trim()
                .replaceAll("\\s+", " ")
                .replaceAll(";+$", "");
    }

    public static DeleteCommand parseDelete(String sql) {

        sql = normalize(sql);
        String upper = sql.toUpperCase();

        if (!upper.startsWith("DELETE FROM")) {
            throw new IllegalArgumentException("Not a DELETE statement");
        }

        // DELETE FROM users WHERE id=1 AND name='Bob'
        String table =
                sql.split(" ")[2];

        String wherePart =
                upper.contains("WHERE")
                        ? sql.substring(upper.indexOf("WHERE") + 5).trim()
                        : "";

        Map<String, Object> whereEquals =
                wherePart.isEmpty()
                        ? Map.of()
                        : parseConditions(wherePart);

        return new DeleteCommand(table, whereEquals);
    }

    public static SelectCommand parseSelect(String sql) {

        sql = normalize(sql);
        String upper = sql.toUpperCase();

        if (!upper.startsWith("SELECT * FROM")) {
            throw new IllegalArgumentException("Only SELECT * FROM is supported");
        }

        // SELECT * FROM users WHERE id=1 AND name='Bob'
        String table =
                sql.split(" ")[3];

        String wherePart =
                upper.contains("WHERE")
                        ? sql.substring(upper.indexOf("WHERE") + 5).trim()
                        : "";

        Map<String, Object> whereEquals =
                wherePart.isEmpty()
                        ? Map.of()
                        : parseConditions(wherePart);

        return new SelectCommand(table, whereEquals);
    }

    private static List<String> parseColumnList(String input) {
        List<String> cols = new ArrayList<>();
        for (String part : input.split(",")) {
            cols.add(part.trim());
        }
        return cols;
    }

    private static List<Object> parseValueList(String input) {
        List<Object> vals = new ArrayList<>();
        for (String part : input.split(",")) {
            vals.add(parseValue(part.trim()));
        }
        return vals;
    }


    public static InsertCommand parseInsert(String sql) {

        sql = normalize(sql);
        String upper = sql.toUpperCase();

        if (!upper.startsWith("INSERT INTO")) {
            throw new IllegalArgumentException("Not an INSERT statement");
        }

        // INSERT INTO users (id, name) VALUES (1, 'Bob')
        String table =
                sql.split(" ")[2];

        int colsStart = sql.indexOf('(');
        int colsEnd   = sql.indexOf(')', colsStart);

        int valsStart = upper.indexOf("VALUES", colsEnd) + 6;
        int valsOpen  = sql.indexOf('(', valsStart);
        int valsClose = sql.indexOf(')', valsOpen);

        if (colsStart == -1 || valsOpen == -1) {
            throw new IllegalArgumentException("Invalid INSERT syntax");
        }

        List<String> columns =
                parseColumnList(
                        sql.substring(colsStart + 1, colsEnd)
                );

        List<Object> values =
                parseValueList(
                        sql.substring(valsOpen + 1, valsClose)
                );

        if (columns.size() != values.size()) {
            throw new IllegalArgumentException(
                    "Column count does not match value count"
            );
        }

        return new InsertCommand(table, columns, values);
    }

    public static JoinCommand parseJoin(String sql) {

        sql = normalize(sql);
        String upper = sql.toUpperCase();

        if (!upper.startsWith("SELECT * FROM")) {
            throw new IllegalArgumentException("Only SELECT * FROM ... JOIN supported");
        }

        JoinType joinType;
        if (upper.contains(" LEFT JOIN ")) {
            joinType = JoinType.LEFT;
        } else if (upper.contains(" RIGHT JOIN ")) {
            joinType = JoinType.RIGHT;
        } else if (upper.contains(" JOIN ")) {
            joinType = JoinType.INNER;
        } else {
            throw new IllegalArgumentException("No JOIN clause found");
        }

        // SELECT * FROM orders LEFT JOIN users ON ...
        String[] tokens = sql.split(" ");

        String leftTable = tokens[3];
        String rightTable =
                joinType == JoinType.INNER
                        ? tokens[5]
                        : tokens[6];

        int onIndex = upper.indexOf("ON") + 2;
        int whereIndex =
                upper.contains("WHERE")
                        ? upper.indexOf("WHERE")
                        : sql.length();

        String onClause =
                sql.substring(onIndex, whereIndex).trim();

        // orders.user_id = users.id
        String[] onParts = onClause.split("=");

        String[] leftRef = onParts[0].trim().split("\\.");
        String[] rightRef = onParts[1].trim().split("\\.");

        Map<String, Object> whereEquals =
                upper.contains("WHERE")
                        ? parseConditions(
                        sql.substring(whereIndex + 5).trim()
                )
                        : Map.of();

        return new JoinCommand(
                joinType,
                leftTable,
                rightTable,
                leftRef[1],
                rightRef[1],
                whereEquals
        );
    }

}
