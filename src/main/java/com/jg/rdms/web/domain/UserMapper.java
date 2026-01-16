package com.jg.rdms.web.domain;


import java.util.HashMap;
import java.util.Map;

public class UserMapper {

    public static Map<String, Object> toRow(User user) {
        Map<String, Object> row = new HashMap<>();
        row.put("name", user.getName());
//        row.put("email", user.getEmail());
//        row.put("password", user.getPassword());
        return row;
    }

    public static User fromRow(Map<String, Object> row) {
        return new User(
                (Integer) row.get("id"),
                (String) row.get("name")
//                (String) row.get("email"),
//                (String) row.get("password")
        );
    }
}

