package no.ks.fiks.database.service.user.crud;

import no.ks.fiks.database.service.user.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

import static java.util.Optional.ofNullable;

@Service
public class InDatabaseUsers implements UserCrudService {

    @Autowired
    JdbcTemplate jdbcTemplate;


    @Override
    public User save(User user) {
        return null;
    }

    @Override
    public Optional<User> findByUsername(String username) {
        User user = jdbcTemplate.queryForObject("SELECT * FROM ssbks.Users WHERE username = '" + username + "'", User.class);
        return ofNullable(user);
    }

    private boolean checkForTable() {
        try {
            int result = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'Users' AND TABLE_SCHEMA = 'ssbks'", Integer.class);
            if (result == 1)
                return true;

        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }
        return false;
    }

    private boolean createUserTable() {
        if (!checkForTable()) {
            jdbcTemplate.execute("CREATE TABLE ssbks.Users (username varchar(100), password varchar(100))");
            return true;
        }
        return false;
    }
}
