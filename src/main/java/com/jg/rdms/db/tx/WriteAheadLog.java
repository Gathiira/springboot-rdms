package com.jg.rdms.db.tx;

import java.io.*;

public class WriteAheadLog {
    private final ObjectOutputStream out;

    public WriteAheadLog(String path) throws IOException {
        out = new ObjectOutputStream(new FileOutputStream(path, true));
    }

    public synchronized void log(WalRecord record) throws IOException {
        out.writeObject(record);
        out.flush();
    }

    public void delete() {
        // no-op for now
    }

}
