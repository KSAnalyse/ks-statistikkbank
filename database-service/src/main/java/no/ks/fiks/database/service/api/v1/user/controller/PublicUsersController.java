package no.ks.fiks.database.service.api.v1.user.controller;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import no.ks.fiks.database.service.api.v1.auth.api.UserAuthenticationService;
import no.ks.fiks.database.service.api.v1.user.crud.api.UserCrudService;
import no.ks.fiks.database.service.api.v1.user.entity.User;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;

@RestController
@RequestMapping("/public/users")
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
                        .password(BCrypt.hashpw(password, BCrypt.gensalt(10)))
                        .build()
        );
        return "User " + username + " created in the database.";
    }

    @PostMapping("/login")
    String login(
            @RequestParam("email") final String username,
            @RequestParam("password") final String password) {
        System.out.println("Test");
        return authentication
                .login(username, password)
                .orElseThrow(() -> new RuntimeException("invalid login and/or password"));
    }
}
