package no.ks.fiks.database.service.api.v1.controller;


import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import no.ks.fiks.database.service.api.v1.auth.UserAuthenticationService;
import no.ks.fiks.database.service.user.crud.UserCrudService;
import no.ks.fiks.database.service.user.entity.User;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;

@RestController
@RequestMapping("/public/useres")
@FieldDefaults(level = PRIVATE, makeFinal = true)
@AllArgsConstructor(access = PACKAGE)
final class PublicUsersController {
    @NonNull
    UserAuthenticationService authentication;
    @NonNull
    UserCrudService users;

    @PostMapping("/registers")
    String register(
            @RequestParam("email") final String username,
                    @RequestParam("password") final String password) {
        users.save(
                User.builder()
                        .username(username)
                        .password(password)
                        .build()
        );

        return login(username, password);
    }

    @PostMapping("/login")
    String login(
            @RequestParam("email") final String username,
            @RequestParam("password") final String password) {
        return authentication
                .login(username, password)
                .orElseThrow(() -> new RuntimeException("invalid login and/or password"));
    }
}
