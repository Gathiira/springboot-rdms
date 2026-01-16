package com.jg.rdms.db.repl;

import com.jg.rdms.db.sql.QueryExecutor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

public class Repl {

    private final QueryExecutor executor;

    public Repl(QueryExecutor executor) {
        this.executor = executor;
    }

    public void start() {
        System.out.println("RDMS REPL — type 'exit' to quit");

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(System.in))) {

            String line;
            while (true) {
                System.out.print("rdms> ");
                line = reader.readLine();

                if (line == null) {
                    break;
                }

                line = line.trim();

                if (line.isEmpty()) {
                    continue;
                }

                if (line.equalsIgnoreCase("exit")
                        || line.equalsIgnoreCase("quit")) {
                    System.out.println("Bye.");
                    break;
                }

                try {
                    Object result = executor.execute(line);

                    if (result instanceof List<?> rows) {
                        ResultPrinter.printRows(
                                (List<Map<String, Object>>) rows
                        );
                    } else if (result instanceof Integer count) {
                        System.out.println(count + " row(s) affected");
                    } else if (result == null) {
                        System.out.println("OK");
                    } else {
                        System.out.println(result);
                    }

                } catch (Exception e) {
                    System.err.println("ERROR: " + e.getMessage());
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
