package com.jg.rdms.db;

import com.jg.rdms.db.core.Database;
import com.jg.rdms.db.core.PersistentTable;
import com.jg.rdms.db.core.Table;
import com.jg.rdms.db.repl.Repl;
import com.jg.rdms.db.sql.QueryExecutor;
import com.jg.rdms.db.tx.TransactionManager;

public class RdbmsApp {
    public static void main(String[] args) {
        Database db = new Database();

        // 🔁 LOAD USER SCHEMAS (if restarting)
        db.init();
        System.out.println("Database startup completed");

        TransactionManager txManager = new TransactionManager();
        QueryExecutor executor = new QueryExecutor(db, txManager);

        new Repl(executor).start();
    }
}
