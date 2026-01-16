package com.jg.rdms.db.storage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HeapFile {
    private final Path path;
    private final RandomAccessFile file;

    public HeapFile(String path) throws IOException {
        this.path = Path.of(path);
        this.file = new RandomAccessFile(this.path.toFile(), "rw");
    }

    public synchronized long append(Map<String, Object> row) throws IOException {
        byte[] data = RowSerializer.serialize(row);
        long offset = file.length();
        file.seek(offset);
        file.writeInt(data.length);
        file.write(data);
        return offset;
    }

    public synchronized List<Map<String, Object>> readAll() throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        file.seek(0);

        while (file.getFilePointer() < file.length()) {
            int size = file.readInt();
            byte[] data = new byte[size];
            file.readFully(data);
            result.add(RowSerializer.deserialize(data));
        }

        return result;
    }

    public void delete() {
        try {
            file.close();
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void truncate() {
        try {
            file.setLength(0);   // THIS is the key line
            file.seek(0);
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate heap file", e);
        }
    }

}
