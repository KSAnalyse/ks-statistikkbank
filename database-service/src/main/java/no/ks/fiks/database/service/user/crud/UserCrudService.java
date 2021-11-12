package no.ks.fiks.database.service.user.crud;

import no.ks.fiks.database.service.user.entity.User;

import java.util.Optional;

public interface UserCrudService {

    User save(User user);

    Optional<User> findByUsername(String username);
}
