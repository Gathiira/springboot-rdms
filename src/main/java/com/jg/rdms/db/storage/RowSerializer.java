package com.jg.rdms.db.storage;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class RowSerializer {

    public static byte[] serialize(Map<String, Object> row) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        out.writeInt(row.size());

        for (var e : row.entrySet()) {
            out.writeUTF(e.getKey());

            Object value = e.getValue();

            if (value == null) {
                out.writeChar('N'); // NULL marker
            } else if (value instanceof Integer i) {
                out.writeChar('I');
                out.writeInt(i);
            } else {
                out.writeChar('S');
                out.writeUTF(value.toString());
            }
        }

        return bos.toByteArray();
    }

    public static Map<String, Object> deserialize(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

        int size = in.readInt();
        Map<String, Object> row = new HashMap<>();

        for (int i = 0; i < size; i++) {
            String col = in.readUTF();
            char type = in.readChar();

            Object value;
            switch (type) {
                case 'N' -> value = null;
                case 'I' -> value = in.readInt();
                case 'S' -> value = in.readUTF();
                default -> throw new IOException(
                        "Unknown column type marker: " + type
                );
            }

            row.put(col, value);
        }

        return row;
    }
}
