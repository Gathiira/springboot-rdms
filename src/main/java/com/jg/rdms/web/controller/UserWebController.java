package com.jg.rdms.web.controller;

import com.jg.rdms.web.service.UserService;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping
@AllArgsConstructor
public class UserWebController {

    private final UserService service;

    @GetMapping
    public String list(Model model) {
        Object users = service.findAll();
        model.addAttribute("users", users);
        return "list";
    }

    @GetMapping("/new")
    public String createForm() {
        return "new";
    }

    @PostMapping
    public String create(@RequestParam String name) {
        service.create(name);
        return "redirect:/";
    }

    @GetMapping("/{id}/edit")
    public String editForm(@PathVariable int id, Model model) {
        Object users = service.getUser(id);
        model.addAttribute("user", users);
        return "edit";
    }

    @PostMapping("/{id}")
    public String update(
            @PathVariable String id,
            @RequestParam String name
    ) {
        service.updateUser(id, name);
        return "redirect:/";
    }

    @PostMapping("/{id}/delete")
    public String delete(@PathVariable String id) {
        service.deleteUser(id);
        return "redirect:/";
    }
}
