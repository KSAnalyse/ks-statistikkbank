package no.ks.fiks.database.service.api.v1.user.auth;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import no.ks.fiks.database.service.api.v1.auth.api.UserAuthenticationService;
import no.ks.fiks.database.service.api.v1.token.api.TokenService;
import no.ks.fiks.database.service.api.v1.user.crud.api.UserCrudService;
import no.ks.fiks.database.service.api.v1.user.entity.User;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.stereotype.Service;

import java.util.Optional;

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;

@Service
@AllArgsConstructor(access = PACKAGE)
@FieldDefaults(level = PRIVATE, makeFinal = true)
final class TokenAuthenticationService implements UserAuthenticationService {
    @NonNull
    TokenService tokens;
    @NonNull
    UserCrudService users;

    @Override
    public Optional<String> login(final String username, final String password) {
        Optional<String> tokenID = users
                    .findByUsername(username)
                    .filter(user -> BCrypt.checkpw(password, user.getPassword()))
                    .map(user -> tokens.expiring(ImmutableMap.of("username", username)));
            //users.save(User.builder().id(tokenID.orElse(null)).username(username).password(password).build());
       return tokenID;
    }

    @Override
    public Optional<User> findByToken(final String token) {
        return Optional
                .of(tokens.verify(token))
                .map(map -> map.get("username"))
                .flatMap(users::findByUsername);
    }

    @Override
    public void logout(final User user) {
        users.delete(user);
    }
}