package com.jg.rdms.web.service;

import com.jg.rdms.db.core.Database;
import com.jg.rdms.db.sql.QueryExecutor;
import com.jg.rdms.db.tx.TransactionManager;
import com.jg.rdms.web.domain.User;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class UserService {

    private final Database db;
    private final TransactionManager txManager;

    public User create(User user) {
        QueryExecutor executor = new QueryExecutor(db, txManager);
        executor.execute(
                String.format("INSERT INTO users (name) VALUES ('%s')", user.getName())
        );
        return user;
    }

    public User updateUser(String userId, User user) {
        QueryExecutor executor = new QueryExecutor(db, txManager);
        executor.execute(
                String.format("UPDATE users SET name='%s' WHERE id=%s ", user.getName(), userId)
        );
        return user;
    }

    public void deleteUser(String userId) {
        QueryExecutor executor = new QueryExecutor(db, txManager);
        executor.execute(
                String.format("DELETE FROM users WHERE id=%s", userId)
        );
    }

    public Object getUser(String userId) {
        QueryExecutor executor = new QueryExecutor(db, txManager);
        return executor.execute(
                String.format("SELECT * FROM users WHERE id=%s", userId)
        );
    }

    public Object findAll() {
        QueryExecutor executor = new QueryExecutor(db, txManager);
        return executor.execute(
                "SELECT * FROM users"
        );
    }
}