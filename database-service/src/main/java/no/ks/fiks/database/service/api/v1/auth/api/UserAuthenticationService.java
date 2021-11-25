package no.ks.fiks.database.service.api.v1.auth.api;

import no.ks.fiks.database.service.api.v1.user.entity.User;

import java.util.Optional;

public interface UserAuthenticationService {

    Optional<String> login(String username, String password);

    Optional<User> findByToken(String token);

    void logout(User user);
}
