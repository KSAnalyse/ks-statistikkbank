package no.ks.fiks.database.service.api.v1.token.api;

import java.util.Map;

public interface TokenService {

    String permanent(Map<String, String> attributes);

    String expiring(Map<String, String> attributes);

    Map<String, String> untrusted(String token);

    Map<String, String> verify(String token);
}
