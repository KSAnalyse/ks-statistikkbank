package no.ks.fiks.database.service.api.v1.user.crud.in.database;

import no.ks.fiks.database.service.api.v1.user.crud.api.UserCrudService;
import no.ks.fiks.database.service.api.v1.user.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

@Service
public class InDatabaseUsers implements UserCrudService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public User save(final User user) {
        jdbcTemplate.update("UPDATE config.Users SET id = '" + user.getId() + "' WHERE username = '" + user.getUsername() + "'");
        return user;

    }

    @Override
    public User delete(final User user) {
        jdbcTemplate.update("UPDATE config.Users SET id = NULL WHERE username = '" + user.getUsername() + "'");
        return user;
    }

    @Override
    public Optional<User> find(String id) {
        String sql = "SELECT * FROM config.Users WHERE id = '" + id + "'";
        List<Map<String, Object>> row = jdbcTemplate.queryForList(sql);
        final User user = User.builder()
                .id(row.get(0).get("id") == null ? "null" : row.get(0).get("id").toString())
                .username(row.get(0).get("username").toString())
                .password(row.get(0).get("password").toString())
                .build();
        return ofNullable(user);
    }

    @Override
    public Optional<User> findByUsername(String username) {
        String sql = "SELECT * FROM config.Users WHERE username = '" + username + "'";
        List<Map<String, Object>> row = jdbcTemplate.queryForList(sql);
        User user = User.builder()
                .id(row.get(0).get("id") == null ? "null" : row.get(0).get("id").toString())
                .username(row.get(0).get("username").toString())
                .password(row.get(0).get("password") == null ? "null" : row.get(0).get("id").toString())
                .build();
        return ofNullable(user);
    }
}
