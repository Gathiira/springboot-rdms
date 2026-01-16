package com.jg.rdms.db.index;

import java.util.*;

public class Index {

    private final Map<Object, Long> map = new HashMap<>();

    public void put(Object key, long offset) {
        map.put(key, offset);
    }

    public Long get(Object key) {
        return map.get(key);
    }

    public void remove(Object key) {
        map.remove(key);
    }

    public Set<Object> keys() {
        return map.keySet();
    }
}
