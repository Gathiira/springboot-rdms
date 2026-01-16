package com.jg.rdms.db.storage;

import lombok.Getter;

import java.io.*;
import java.nio.file.*;
import java.util.*;

@Getter
public class HeapFile {

    private final Path path;

    public HeapFile(String path) throws IOException {
        this.path = Path.of(path);
        Files.createDirectories(this.path.getParent());
        if (!Files.exists(this.path)) {
            Files.createFile(this.path);
        }
    }

    /* =========================
       APPEND (SAFE)
       ========================= */

    public synchronized long append(Map<String, Object> row) throws IOException {

        byte[] data = RowSerializer.serialize(row);

        try (RandomAccessFile file =
                     new RandomAccessFile(path.toFile(), "rw")) {

            long offset = file.length();
            file.seek(offset);

            file.writeInt(data.length);
            file.write(data);

            file.getFD().sync(); // 🔑 CRITICAL

            return offset;
        }
    }

    /* =========================
       READ
       ========================= */

    public synchronized List<Map<String, Object>> readAll() throws IOException {

        List<Map<String, Object>> result = new ArrayList<>();

        try (RandomAccessFile file =
                     new RandomAccessFile(path.toFile(), "r")) {

            while (file.getFilePointer() < file.length()) {

                int size = file.readInt();

                if (size <= 0 || size > 10_000_000) {
                    throw new IllegalStateException(
                            "Corrupt heap record size: " + size
                    );
                }

                byte[] data = new byte[size];
                file.readFully(data);

                result.add(RowSerializer.deserialize(data));
            }
        }

        return result;
    }

    /* =========================
       TRUNCATE
       ========================= */

    public synchronized void truncate() {
        try (RandomAccessFile file =
                     new RandomAccessFile(path.toFile(), "rw")) {

            file.setLength(0);
            file.getFD().sync();

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to truncate heap file", e
            );
        }
    }

    /* =========================
       DELETE
       ========================= */

    public void delete() {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
