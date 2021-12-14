package no.ks.fiks.database.service.api.v1.user.controller;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import no.ks.fiks.database.service.api.v1.auth.api.UserAuthenticationService;
import no.ks.fiks.database.service.api.v1.user.crud.api.UserCrudService;
import no.ks.fiks.database.service.api.v1.user.entity.User;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;

@RestController
@RequestMapping("/public/users")
@FieldDefaults(level = PRIVATE, makeFinal = true)
@AllArgsConstructor(access = PACKAGE)
final class PublicUsersController {
    @NonNull
    UserAuthenticationService userAuthenticationService;
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
    String login(HttpServletRequest request) {
        String authorization = request.getHeader("Authorization");
        String username = "";
        String password = "";
        if (authorization != null && authorization.toLowerCase().startsWith("basic")) {
            String base64Credentials = authorization.substring("Basic".length()).trim();
            byte[] credentialsDecoded = Base64.getDecoder().decode(base64Credentials);
            String credentials = new String(credentialsDecoded, StandardCharsets.UTF_8);
            final String[] values = credentials.split(":", 2);
            username = values[0];
            password = values[1];
        }
        return userAuthenticationService
                .login(username, password)
                .orElseThrow(() -> new RuntimeException("invalid login and/or password"));
    }
}
