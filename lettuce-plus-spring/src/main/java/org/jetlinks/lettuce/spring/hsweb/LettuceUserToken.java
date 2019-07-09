package org.jetlinks.lettuce.spring.hsweb;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.output.BooleanOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.token.TokenState;
import org.hswebframework.web.authorization.token.UserToken;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.codec.StringCodec;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class LettuceUserToken implements UserToken {

    private transient LettucePlus plus;

    private transient String redisKey;

    private transient String token;

    private transient String redisPrefix;

    private transient Map<String, Object> localCache = new ConcurrentHashMap<>();

    @SneakyThrows
    private <K, V> StatefulRedisConnection<K, V> getRedis() {
        return plus.<K, V>getConnection().toCompletableFuture().get();
    }

    public LettuceUserToken(String redisPrefix, String token, LettucePlus plus) {
        this.token = token;
        this.redisKey = redisPrefix + ":user-token:".concat(token);
        this.plus = plus;
        this.redisPrefix = redisPrefix;
    }

    enum NullValue {
        instance
    }

    @SuppressWarnings("all")
    private <T> T tryGetFromLocalCache(String key) {
        Object v = localCache
                .computeIfAbsent(key, _key -> Optional.ofNullable(this.<String, Object>getRedis().sync().hget(redisKey, _key))
                        .orElse(NullValue.instance));
        if (v == NullValue.instance) {
            return null;
        }
        return ((T) v);
    }

    @Override
    public String getUserId() {

        return Optional.<String>ofNullable(tryGetFromLocalCache("userId")).orElse("");
    }

    @Override
    public String getToken() {
        return token;
    }

    @Override
    @SneakyThrows
    public long getRequestTimes() {
        AsyncCommand<String, Object, Object> command = new AsyncCommand<>(new Command<>(CommandType.HGET,
                new ValueOutput<>(StringCodec.getInstance()),
                new CommandArgs<>(plus.<String, Object>getDefaultCodec())
                        .addKeys(redisKey, "requestTimes")));

        this.<String, Object>getRedis().dispatch(command);

        Object val = command.get();

        return val == null ? 0 : (val instanceof Number ? ((Number) val).longValue() : Long.parseLong(String.valueOf(val)));
    }

    @Override
    public long getLastRequestTime() {
        Long val = this.<String, Long>getRedis()
                .sync()
                .hget(redisKey, "lastRequestTime");

        return val == null ? 0 : val;
    }

    @Override
    public long getSignInTime() {
        return Optional.<Long>ofNullable(tryGetFromLocalCache("signInTime"))
                .orElse(-1L);
    }

    @Override
    public TokenState getState() {

        TokenState state = Optional.<TokenState>ofNullable(tryGetFromLocalCache("state"))
                .orElse(TokenState.expired);
        if (state == TokenState.normal) {
            if (!isAlive()) {
                localCache.put("state", TokenState.expired);

                return TokenState.expired;
            }
        }
        return state;
    }

    @Override
    public String getType() {
        return tryGetFromLocalCache("type");
    }

    @Override
    public long getMaxInactiveInterval() {
        return Optional.<Long>ofNullable(tryGetFromLocalCache("maxInactiveInterval"))
                .orElse(-1L);
    }

    void remove() {
        String script = "" +
                "redis.call('del', KEYS[1]); " +
                "redis.call('srem', KEYS[2], KEYS[4]);" +
                "redis.call('publish',KEYS[3],ARGV[1]);" +
                "return 1;";

        AsyncCommand<Object, Object, Boolean> command = new AsyncCommand<>(new Command<>(EvalCommand.EVAL,
                new BooleanOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.getDefaultCodec())
                        .add(script)
                        .add(4)
                        .addKeys(redisKey,
                                redisPrefix.concat(":user-tokens:").concat(getUserId()),
                                redisPrefix.concat(":user-token-changed"),
                                token)
                        .addValues(token)));

        this.getRedis().dispatch(command);

        command.whenComplete((nil, error) -> {
            if (null != error) {
                log.error("remove token {} error", token, error);
            }
        });
        localCache.clear();
    }

    void updateStateCache() {
        localCache.remove("state");
    }

    boolean isAlive() {
        return this.getRedis().sync().exists(redisKey) > 0;
    }

    public void changeState(TokenState state) {
        localCache.put("state", state);
        String script = "" +
                "if redis.call('exists', KEYS[1]) == 0 then " +
                "return 0;" +
                "else " +
                "redis.call('hset', KEYS[1], KEYS[2], ARGV[1]);" +
                "redis.call('publish',KEYS[3],ARGV[2]);" +
                "return 1;" +
                "end";

        AsyncCommand<Object, Object, Boolean> command = new AsyncCommand<>(new Command<>(EvalCommand.EVAL,
                new BooleanOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, "state", redisPrefix.concat(":user-token-changed"))
                        .addValues(state, token)));

        this.getRedis().dispatch(command);

        command.whenComplete((nil, error) -> {
            if (null != error) {
                log.error("touch token {} error", token, error);
            }
        });
    }

    void doSign(String userId, String type, long maxInactiveInterval) {
        Map<String, Object> all = new HashMap<>();

        all.put("userId", userId);
        all.put("state", TokenState.normal);
        all.put("maxInactiveInterval", maxInactiveInterval);
        all.put("signInTime", System.currentTimeMillis());
        all.put("type", type);
        all.put("token", token);

        this.<String, Object>getRedis()
                .sync()
                .hmset(redisKey, all);
        touch();
    }


    public void touch() {

        long maxInactiveInterval = getMaxInactiveInterval();
        String script = "" +
                "if redis.call('exists', KEYS[1]) == 0 then " +
                "return 0;" +
                "else " +
                "redis.call('hset',KEYS[1],KEYS[2],ARGV[1]);" +//lastRequestTime
                "redis.call('hincrby',KEYS[1],KEYS[3],1);" +//访问次数自增
                (maxInactiveInterval > 0 ? ("redis.call('pexpire',KEYS[1]," + maxInactiveInterval + ");") : "") + // 设置过期时间
                "return 1;" +
                "end";

        AsyncCommand<Object, Object, Boolean> command = new AsyncCommand<>(new Command<>(EvalCommand.EVAL,
                new BooleanOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, "lastRequestTime", "requestTimes")
                        .addValues(System.currentTimeMillis())));

        this.getRedis().dispatch(command);

        command.whenComplete((nil, error) -> {
            if (null != error) {
                log.error("touch token {} error", token, error);
            } else if (Boolean.FALSE.equals(nil)) {
                log.warn("touch token {} fail", token);
            }
        });

    }

    public SerializableUserToken toSerializable() {
        return SerializableUserToken.of(getUserId(), getToken(), getRequestTimes(), getLastRequestTime(), getSignInTime(), getState(), getType(), getMaxInactiveInterval());
    }

    enum EvalCommand implements ProtocolKeyword {
        EVAL;

        private final byte[] name = name().getBytes();

        @Override
        public byte[] getBytes() {
            return name;
        }

    }
}
