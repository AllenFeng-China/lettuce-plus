package org.jetlinks.lettuce.spring.hsweb;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hswebframework.web.authorization.token.TokenState;
import org.hswebframework.web.authorization.token.UserToken;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class SerializableUserToken implements UserToken {

    private String userId;

    private String token;

    private long requestTimes;

    private long lastRequestTime;

    private long signInTime;

    private TokenState state;

    private String type;

    private long maxInactiveInterval;


}
