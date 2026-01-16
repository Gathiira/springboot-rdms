package com.jg.rdms.web.service;

import com.jg.rdms.db.core.Database;
import com.jg.rdms.db.sql.QueryExecutor;
import com.jg.rdms.db.tx.TransactionManager;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class UserService {

    private final QueryExecutor executor;

    public UserService(Database db, TransactionManager txManager) {
        this.executor = new QueryExecutor(db, txManager);
    }

    public String create(String name) {
        executor.execute(
                String.format("INSERT INTO users (name) VALUES ('%s')", name)
        );
        return name;
    }

    public String updateUser(String userId, String name) {
        executor.execute(
                String.format("UPDATE users SET name='%s' WHERE id=%s ", name, userId)
        );
        return name;
    }

    public void deleteUser(String userId) {
        executor.execute(
                String.format("DELETE FROM users WHERE id=%s", userId)
        );
    }

    public Map<String, Object> getUser(int id) {
        List<Map<String, Object>> result = (List<Map<String, Object>>) executor.execute(
                "SELECT * FROM users WHERE id=" + id
        );

        return result.isEmpty() ? null : result.get(0);
    }

    public Object findAll() {
        return executor.execute(
                "SELECT * FROM users"
        );
    }
}