package org.jetlinks.lettuce.spring.hsweb;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.exception.AccessDenyException;
import org.hswebframework.web.authorization.token.AllopatricLoginMode;
import org.hswebframework.web.authorization.token.TokenState;
import org.hswebframework.web.authorization.token.UserToken;
import org.hswebframework.web.authorization.token.UserTokenManager;
import org.hswebframework.web.authorization.token.event.UserTokenChangedEvent;
import org.hswebframework.web.authorization.token.event.UserTokenCreatedEvent;
import org.hswebframework.web.authorization.token.event.UserTokenRemovedEvent;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.codec.StringCodec;
import org.jetlinks.lettuce.supports.DefaultRedisLocalCacheMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class LettuceUserTokenManager implements UserTokenManager {

    private LettucePlus plus;

    private final Map<String, SoftReference<LettuceUserToken>> tokenStore = new ConcurrentHashMap<>();

    @Getter
    @Setter
    private Map<String, AllopatricLoginMode> allopatricLoginModes = new HashMap<>();

    //异地登录模式，默认允许异地登录
    private AllopatricLoginMode allopatricLoginMode = AllopatricLoginMode.allow;

    //事件转发器
    private ApplicationEventPublisher eventPublisher;

    private final String redisPrefix;

    public LettuceUserTokenManager(String prefix, LettucePlus plus) {
        this.plus = plus;
        this.redisPrefix = prefix.intern();

        plus.<String>getTopic(redisPrefix.concat(":user-token-changed"))
                .addListener((channel, token) -> Optional.ofNullable(token)
                        .map(this.tokenStore::get)
                        .map(SoftReference::get)
                        .ifPresent(userToken -> {
                            userToken.updateStateCache();
                            // UserToken serializable= userToken.toSerializable();
                            publishEvent(new UserTokenChangedEvent(userToken, userToken));
                        }));

    }

    @Autowired(required = false)
    public void setEventPublisher(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void setAllopatricLoginMode(AllopatricLoginMode allopatricLoginMode) {
        this.allopatricLoginMode = allopatricLoginMode;
    }

    public AllopatricLoginMode getAllopatricLoginMode() {
        return allopatricLoginMode;
    }

    protected void publishEvent(ApplicationEvent event) {
        if (null != eventPublisher) {
            eventPublisher.publishEvent(event);
        }
    }

    @SneakyThrows
    public <K, V> StatefulRedisConnection<K, V> getRedis() {
        return plus.<K, V>getConnection().toCompletableFuture().get();
    }

    @Override
    public LettuceUserToken getByToken(String token) {

        SoftReference<LettuceUserToken> reference = tokenStore.get(token);
        LettuceUserToken _token;
        if (reference != null && (_token = reference.get()) != null) {
            return _token;
        }
        _token = new LettuceUserToken(redisPrefix, token, plus);

        tokenStore.put(token, new SoftReference<>(_token));
        return _token;
    }

    @Override
    @SneakyThrows
    public List<UserToken> getByUserId(String userId) {

        return plus.<String, String>getRedisAsync(StringCodec.getInstance())
                .thenCompose(redis -> redis.smembers(redisPrefix.concat(":user-tokens:").concat(userId)))
                .thenApply(tokens -> tokens.parallelStream()
                        .map(this::getByToken)
                        .filter(Objects::nonNull)
                        .map(token -> ((UserToken) token))
                        .collect(Collectors.toList()))
                .toCompletableFuture()
                .get();
    }

    @Override
    @SneakyThrows
    public boolean userIsLoggedIn(String userId) {

        String script = "" +
                "local tokens = redis.call('smembers', KEYS[1]);" +
                "for i = 1, #tokens, 1 do " +
                "   local key = KEYS[2]..':user-token:'..tokens[i];" +
                "   local state = redis.call('hget',key,'state');" +
                "   if state == nil then " +
                "       redis.call('srem',KEYS[1],tokens[i]);" +
                "   end;" +
                "   if state == ARGV[1] then " +
                "       return 1;" +
                "   end;" +
                "end;" +
                "return 0;";


        return plus.<Boolean>eval(script, ScriptOutputType.BOOLEAN,
                new Object[]{redisPrefix.concat(":user-tokens:").concat(userId), redisPrefix}, TokenState.normal)
                .toCompletableFuture()
                .get();
    }

    @Override
    public boolean tokenIsLoggedIn(String token) {
        if (token == null) {
            return false;
        }
        UserToken userToken = getByToken(token);

        return userToken != null && !userToken.isExpired();
    }

    @Override
    public long totalUser() {
        return tokenStore.size();
    }

    @Override
    public long totalToken() {
        return tokenStore.size();
    }

    @Override
    public List<UserToken> allLoggedUser() {
        List<UserToken> all = new ArrayList<>();
        allLoggedUser(all::add);
        return all;
    }

    @Override
    public void allLoggedUser(Consumer<UserToken> consumer) {
        String cursor = "0";
        ScanArgs args = ScanArgs.Builder
                .matches(redisPrefix.concat("*"))
                .limit(1000);
        while (true) {
            KeyScanCursor<String> scanCursor = this.<String, Object>getRedis()
                    .sync()
                    .scan(ScanCursor.of(cursor), args);
            for (String key : scanCursor.getKeys()) {
                consumer.accept(getByToken(key));
            }
            if (scanCursor.isFinished() || scanCursor.getKeys().isEmpty() || "0".equals(scanCursor.getCursor())) {
                break;
            }
            cursor = scanCursor.getCursor();
        }
    }

    @Override
    public void signOutByUserId(String userId) {
        getByUserId(userId)
                .parallelStream()
                .map(LettuceUserToken.class::cast)
                .peek(token -> publishEvent(new UserTokenRemovedEvent(token.toSerializable())))
                .peek(LettuceUserToken::remove)
                .forEach(token -> tokenStore.remove(token.getToken()));

        getRedis().async().del(redisPrefix.concat(":user-tokens:").concat(userId));

    }

    @Override
    public void signOutByToken(String token) {

        LettuceUserToken userToken = getByToken(token);

        userToken.remove();
        tokenStore.remove(token);

    }

    @Override
    public void changeUserState(String userId, TokenState state) {
        getByUserId(userId)
                .parallelStream()
                .map(LettuceUserToken.class::cast)
                .forEach(token -> changeTokenState(token, state));
    }

    @Override
    public void changeTokenState(String token, TokenState state) {
        changeTokenState(getByToken(token), state);
    }

    @Override
    @SneakyThrows
    public UserToken signIn(String token, String type, String userId, long maxInactiveInterval) {

        AllopatricLoginMode mode = allopatricLoginModes.getOrDefault(type, allopatricLoginMode);
        if (mode == AllopatricLoginMode.deny) {
            //如果在其他地方已经登陆了，则禁止当前登陆
            String script = "" +
                    "local tokens = redis.call('smembers', KEYS[1]);" +
                    "for i = 1, #tokens, 1 do " +
                    "   local key = KEYS[3]..':user-token:'..tokens[i];" +
                    "   local state = redis.call('hget',key,'state');" +
                    "   if state == nil then " +
                    "       redis.call('srem',KEYS[1],tokens[i]);" +
                    "   end;" +
                    "   if state == ARGV[1] and tokens[i] ~= KEYS[2] then " +
                    "       return 0;" + //有其他token存在，则认为已经在其他地方登陆了
                    "   end;" +
                    "end;" +
                    "redis.call('sadd', KEYS[1], KEYS[2]);" +
                    "return 1;";
            boolean success = plus.<Boolean>eval(script, ScriptOutputType.BOOLEAN,
                    new Object[]{redisPrefix.concat(":user-tokens:").concat(userId), token, redisPrefix},
                    TokenState.normal)
                    .toCompletableFuture()
                    .get();
            ;
            if (!success) {
                throw new AccessDenyException("已经在其他地方登陆", TokenState.deny.getValue(), null);
            }

        } else if (mode == AllopatricLoginMode.offlineOther) {

            //踢出其他地方登陆的相同用户
            String script = "" +
                    "local tokens = redis.call('smembers', KEYS[1]);" +
                    "redis.call('sadd', KEYS[1], KEYS[3]);" +
                    "for i = 1, #tokens, 1 do " +
                    "   local key = KEYS[4]..':user-token:'..tokens[i];" +
                    "   local type = redis.call('hmget',key,'type','token');" +
                    "   if type[1] == nil then " +
                    "       redis.call('srem',KEYS[1],tokens[i]);" +
                    "   end;" +
                    "   if type[1] == ARGV[1] and tokens[i] ~= KEYS[3] then " +
                    "       redis.call('hset',key,'state',ARGV[2]);" +
                    "       redis.call('publish',KEYS[2],type[2]);" +
                    "   end;" +
                    "end; " +
                    "return 'success';";

            plus.<Boolean>eval(script, ScriptOutputType.BOOLEAN,
                    new Object[]{
                            //KEYS[1]
                            redisPrefix.concat(":user-tokens:").concat(userId),
                            //KEYS[2]
                            redisPrefix.concat(":user-token-changed"),
                            //KEYS[3]
                            token,
                            //KEYS[4]
                            redisPrefix}, type, TokenState.offline)
                    .whenComplete((success, error) -> {
                        if (null != error) {
                            log.error("offline other user error", error);
                        }
                    });
        } else {

            plus.getRedisAsync(StringCodec.getInstance())
                    .thenCompose(redis -> redis.sadd(redisPrefix.concat(":user-tokens:").concat(userId), token))
                    .toCompletableFuture()
                    .get();

        }
        LettuceUserToken detail = getByToken(token);
        detail.doSign(userId, type, maxInactiveInterval);

        publishEvent(new UserTokenCreatedEvent(detail));
        return detail;
    }

    @Override
    public void touch(String token) {
        getByToken(token).touch();
    }

    @Override
    public void checkExpiredToken() {

    }

    public void changeTokenState(LettuceUserToken token, TokenState state) {
        if (token == null) {
            return;
        }
        token.changeState(state);
    }

    private LettuceUserToken checkToken(LettuceUserToken detail) {
        if (null == detail) {
            return null;
        }
        if (detail.isAlive()) {
            return detail;
        }
        detail.remove();
        tokenStore.remove(detail.getToken());

        return null;
    }
}
