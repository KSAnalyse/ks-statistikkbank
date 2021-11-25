package no.ks.fiks.database.service.api.v1.user.crud.api;

import no.ks.fiks.database.service.api.v1.user.entity.User;

import java.util.Optional;

public interface UserCrudService {

    User save(User user);

    User delete(User user);

    Optional<User> find (String id);

    Optional<User> findByUsername(String username);
}
