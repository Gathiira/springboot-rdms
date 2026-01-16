package com.jg.rdms.db.core;

public record Column(
        String name,
        DataType type,
        boolean primary,
        boolean unique,
        String referencesTable,
        String referencesColumn
) {
    public boolean isForeignKey() {
        return referencesTable != null;
    }
}
