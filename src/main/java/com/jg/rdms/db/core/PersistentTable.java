package com.jg.rdms.db.core;

import com.jg.rdms.db.storage.HeapFile;
import com.jg.rdms.db.storage.RowSerializer;
import com.jg.rdms.db.tx.Transaction;
import com.jg.rdms.db.tx.WalRecord;
import com.jg.rdms.db.tx.WriteAheadLog;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class PersistentTable extends Table {

    private final HeapFile heap;
    private final WriteAheadLog wal;
    private final IdGenerator idGenerator = new IdGenerator();

    private final List<Column> uniqueColumns;
    private final Map<String, Set<Object>> uniqueIndexes = new HashMap<>();

    private final Database database;
    private final List<Column> foreignKeyColumns;

    private boolean loaded = false;


    public PersistentTable(Database database, String name, List<Column> columns) {
        super(name, columns);
        this.database = database;

        try {
            this.heap = new HeapFile("data/" + name + ".tbl");
            this.wal  = new WriteAheadLog("data/wal.log");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.uniqueColumns = columns.stream()
                .filter(c -> c.unique() || c.primary())
                .toList();

        this.foreignKeyColumns = columns.stream()
                .filter(Column::isForeignKey)
                .toList();

        for (Column c : uniqueColumns) {
            uniqueIndexes.put(c.name(), new HashSet<>());
        }
    }

    /* =========================
       INSERT
       ========================= */

    @Override
    public synchronized void insert(Transaction tx, Map<String, Object> row) {

        // 🔒 UNIQUE CONSTRAINTS
        for (Column c : uniqueColumns) {
            Object value = row.get(c.name());
            if (uniqueIndexes.get(c.name()).contains(value)) {
                throw new IllegalStateException(
                        "Unique constraint violation on " + c.name() + " = " + value
                );
            }
        }

        // Generate ID
        if (!row.containsKey("id")) {
            row.put("id", idGenerator.nextId());
        }

        // 🔗 FOREIGN KEY CONSTRAINTS
        enforceForeignKeys(row);

        try {
            if (tx != null) {
                wal.log(new WalRecord(tx.getId(), name, row));
            }

            heap.append(row);

            rows.add(new HashMap<>(row));

            for (Column c : uniqueColumns) {
                uniqueIndexes.get(c.name()).add(row.get(c.name()));
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /* =========================
       UPDATE
       ========================= */

    public synchronized int updateWhere(
            Transaction tx,
            String whereColumn,
            Object matchValue,
            Map<String, Object> newValues
    ) {

        int updated = 0;

        for (Map<String, Object> row : rows) {

            if (!Objects.equals(row.get(whereColumn), matchValue)) {
                continue;
            }

            // 🔒 UNIQUE CONSTRAINTS (only changed values)
            for (Column c : uniqueColumns) {
                if (!newValues.containsKey(c.name())) {
                    continue;
                }

                Object oldVal = row.get(c.name());
                Object newVal = newValues.get(c.name());

                if (!Objects.equals(oldVal, newVal)
                        && uniqueIndexes.get(c.name()).contains(newVal)) {
                    throw new IllegalStateException(
                            "Unique constraint violation on " + c.name() + " = " + newVal
                    );
                }
            }

            // 🔗 FOREIGN KEY CONSTRAINTS (only changed FK columns)
            enforceForeignKeysOnUpdate(row, newValues);

            if (tx != null) {
                writeWal(tx, row);
            }

            // Apply update
            row.putAll(newValues);

            // Update unique indexes
            for (Column c : uniqueColumns) {
                if (newValues.containsKey(c.name())) {
                    uniqueIndexes.get(c.name()).remove(row.get(c.name()));
                    uniqueIndexes.get(c.name()).add(newValues.get(c.name()));
                }
            }

            updated++;
        }

        if (updated > 0) {
            rewriteHeapFromMemory();
            rebuildUniqueIndexes();
        }

        return updated;
    }

    private void writeWal(Transaction tx, Map<String, Object> row) {
        if (tx == null) {
            return;
        }
        try {
            wal.log(new WalRecord(tx.getId(), name, row));
        } catch (IOException e) {
            throw new IllegalStateException(
                    "WAL write failed for table " + name, e
            );
        }
    }

    /* =========================
       FOREIGN KEY ENFORCEMENT
       ========================= */

    private void enforceForeignKeys(Map<String, Object> row) {

        for (Column c : foreignKeyColumns) {
            Object value = row.get(c.name());

            if (value == null) {
                throw new IllegalStateException(
                        "Foreign key column cannot be null: " + c.name()
                );
            }

            Table refTable = database.table(c.referencesTable());
            if (refTable == null) {
                throw new IllegalStateException(
                        "Referenced table does not exist: " + c.referencesTable()
                );
            }

            boolean exists = refTable.selectAll().stream()
                    .anyMatch(r ->
                            Objects.equals(
                                    r.get(c.referencesColumn()),
                                    value
                            )
                    );

            if (!exists) {
                throw new IllegalStateException(
                        "Foreign key violation: " +
                                c.name() + " → " +
                                c.referencesTable() + "(" +
                                c.referencesColumn() + ")"
                );
            }
        }
    }

    private void enforceForeignKeysOnUpdate(
            Map<String, Object> row,
            Map<String, Object> newValues
    ) {
        for (Column c : foreignKeyColumns) {

            if (!newValues.containsKey(c.name())) {
                continue;
            }

            Object newVal = newValues.get(c.name());

            Table refTable = database.table(c.referencesTable());
            if (refTable == null) {
                throw new IllegalStateException(
                        "Referenced table does not exist: " + c.referencesTable()
                );
            }

            boolean exists = refTable.selectAll().stream()
                    .anyMatch(r ->
                            Objects.equals(
                                    r.get(c.referencesColumn()),
                                    newVal
                            )
                    );

            if (!exists) {
                throw new IllegalStateException(
                        "Foreign key violation on update: " +
                                c.name() + " → " +
                                c.referencesTable() + "(" +
                                c.referencesColumn() + ")"
                );
            }
        }
    }

    /* =========================
       RECOVERY
       ========================= */

    public synchronized void loadFromDisk() {
        if (loaded) {
            throw new IllegalStateException(
                    "Table " + name + " loaded twice"
            );
        }

        resetData();

        try {
            for (Map<String, Object> row : heap.readAll()) {
                rows.add(row);

                for (Column c : uniqueColumns) {
                    Object val = row.get(c.name());
                    if (!uniqueIndexes.get(c.name()).add(val)) {
                        throw new IllegalStateException(
                                "Corrupt data: duplicate " + c.name() + " table " + name
                        );
                    }
                }
            }

            rebuildIdGenerator();
            loaded = true;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void rebuildIdGenerator() {
        int maxId = rows.stream()
                .map(r -> (Integer) r.get("id"))
                .max(Integer::compareTo)
                .orElse(0);
        idGenerator.set(maxId);
    }

    /* =========================
       DELETE
       ========================= */

    @Override
    public synchronized void deleteWhere(String column, Object value) {

        boolean deleted = false;

        Iterator<Map<String, Object>> it = rows.iterator();

        while (it.hasNext()) {
            Map<String, Object> row = it.next();

            if (Objects.equals(row.get(column), value)) {
                it.remove();
                deleted = true;
            }
        }

        if (deleted) {
            rewriteHeapFromMemory();
            rebuildUniqueIndexes(); // 🔑 CRITICAL
        }
    }

    /* =========================
       HEAP REWRITE
       ========================= */

    private void rewriteHeapFromMemory() {
        Path original = heap.getPath();
        Path temp = original.resolveSibling(original.getFileName() + ".tmp");

        try (
                RandomAccessFile tmpFile =
                        new RandomAccessFile(temp.toFile(), "rw")
        ) {
            // 1️⃣ Write ALL rows to temp file
            for (Map<String, Object> row : rows) {
                byte[] data = RowSerializer.serialize(row);
                tmpFile.writeInt(data.length);
                tmpFile.write(data);
            }

            // 2️⃣ Force data to disk
            tmpFile.getFD().sync();

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to write temp heap file for table " + name, e
            );
        }

        try {
            // 3️⃣ ATOMIC replace
            Files.move(
                    temp,
                    original,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to atomically replace heap file for table " + name, e
            );
        }
    }

    /* =========================
       DROP TABLE
       ========================= */

    public void deleteFiles() {
        heap.delete();
        wal.delete();
    }

    /* =========================
       INDEX LOOKUP (JOIN)
       ========================= */

    @Override
    public Optional<List<Map<String, Object>>> lookupByColumn(
            String column,
            Object value
    ) {
        if (!uniqueIndexes.containsKey(column)) {
            return Optional.empty();
        }

        if (!uniqueIndexes.get(column).contains(value)) {
            return Optional.of(List.of());
        }

        return Optional.of(
                rows.stream()
                        .filter(r -> Objects.equals(r.get(column), value))
                        .toList()
        );
    }

    public void resetData() {
        rows.clear();
        uniqueIndexes.values().forEach(Set::clear);
    }


    private void rebuildUniqueIndexes() {
        uniqueIndexes.values().forEach(Set::clear);

        for (Map<String, Object> row : rows) {
            for (Column c : uniqueColumns) {
                Object val = row.get(c.name());
                if (!uniqueIndexes.get(c.name()).add(val)) {
                    throw new IllegalStateException(
                            "Unique constraint violated during rebuild on column " + c.name()
                    );
                }
            }
        }
    }

}
