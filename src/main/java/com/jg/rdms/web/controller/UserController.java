package com.jg.rdms.web.controller;

import com.jg.rdms.web.domain.User;
import com.jg.rdms.web.service.UserService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@AllArgsConstructor
public class UserController {

    private final UserService service;

    @GetMapping
    public ResponseEntity<Object> all() {
        return ResponseEntity.ok(service.findAll());
    }

    @PostMapping
    public ResponseEntity<String> create(@RequestBody User user) {
        return ResponseEntity.ok(service.create(user.getName()));
    }

    @PutMapping("/{userId}")
    public ResponseEntity<String> create(@PathVariable String userId, @RequestBody User user) {
        return ResponseEntity.ok(service.updateUser(userId, user.getName()));
    }

    @GetMapping("/{userId}")
    public ResponseEntity<Object> getUser(@PathVariable int userId) {
        return ResponseEntity.ok(service.getUser(userId));
    }

    @DeleteMapping("/{userId}")
    public ResponseEntity<String> deleteUser(@PathVariable String userId) {
        service.deleteUser(userId);
        return ResponseEntity.ok("Success");
    }
}
