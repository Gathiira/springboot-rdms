package com.jg.rdms.db.repl;

import java.util.*;

public class ResultPrinter {

    public static void printRows(List<Map<String, Object>> rows) {

        if (rows.isEmpty()) {
            System.out.println("(empty result)");
            return;
        }

        // Preserve column order
        List<String> columns = new ArrayList<>(rows.get(0).keySet());

        Map<String, Integer> widths = new HashMap<>();
        for (String col : columns) {
            widths.put(col, col.length());
        }

        for (Map<String, Object> row : rows) {
            for (String col : columns) {
                Object val = row.get(col);
                widths.put(
                        col,
                        Math.max(
                                widths.get(col),
                                String.valueOf(val).length()
                        )
                );
            }
        }

        printSeparator(columns, widths);
        printRow(columns, widths, columns);
        printSeparator(columns, widths);

        for (Map<String, Object> row : rows) {
            List<String> values = new ArrayList<>();
            for (String col : columns) {
                values.add(String.valueOf(row.get(col)));
            }
            printRow(columns, widths, values);
        }

        printSeparator(columns, widths);
    }

    private static void printSeparator(
            List<String> cols,
            Map<String, Integer> widths
    ) {
        for (String col : cols) {
            System.out.print("+");
            System.out.print("-".repeat(widths.get(col) + 2));
        }
        System.out.println("+");
    }

    private static void printRow(
            List<String> cols,
            Map<String, Integer> widths,
            List<String> values
    ) {
        for (int i = 0; i < cols.size(); i++) {
            System.out.print("| ");
            System.out.print(
                    pad(values.get(i), widths.get(cols.get(i)))
            );
            System.out.print(" ");
        }
        System.out.println("|");
    }

    private static String pad(String s, int width) {
        return s + " ".repeat(Math.max(0, width - s.length()));
    }
}
