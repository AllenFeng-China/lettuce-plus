package org.jetlinks.lettuce.spring.hsweb;

import lombok.SneakyThrows;
import org.hswebframework.web.authorization.exception.AccessDenyException;
import org.hswebframework.web.authorization.token.AllopatricLoginMode;
import org.hswebframework.web.authorization.token.TokenState;
import org.hswebframework.web.authorization.token.UserToken;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.spring.RedisClientHelper;
import org.jetlinks.lettuce.supports.DefaultLettucePlus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LettuceUserTokenManagerTest {

    private LettuceUserTokenManager tokenManager;

    private LettucePlus plus;

    @Before
    public void init() {
        plus = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());

        tokenManager = new LettuceUserTokenManager("test", plus);

    }

    @Test
    @SneakyThrows
    public void testAllopatricLoginMode() {
        LettuceUserTokenManager tokenManager = new LettuceUserTokenManager("testAllopatricLoginMode", plus);
        try {
            tokenManager.getAllopatricLoginModes().put("test", AllopatricLoginMode.deny);
            tokenManager.getAllopatricLoginModes().put("test2", AllopatricLoginMode.offlineOther);

            tokenManager.signIn("test-token", "test", "admin", 10_000);

            try {
                tokenManager.signIn("test-token-2", "test", "admin", 10_000);
                Assert.fail();
            } catch (AccessDenyException e) {
                System.out.println(e.getMessage());
            }

            UserToken token = tokenManager.signIn("test-token-3", "test2", "admin2", 100_000);

            tokenManager.signIn("test-token-4", "test2", "admin2", 100_000);

            Thread.sleep(1000);
            Assert.assertEquals(token.getState(), TokenState.offline);
        } finally {
            tokenManager.signOutByToken("test-token");
            tokenManager.signOutByToken("test-token-2");
            tokenManager.signOutByToken("test-token-3");
            tokenManager.signOutByToken("test-token-4");
        }

    }

    @Test
    @SneakyThrows
    public void testUserToken() {

        UserToken userToken = tokenManager.signIn("test-token", "test", "admin", 100_000);

        Assert.assertNotNull(userToken);

        Thread.sleep(100);
        Assert.assertTrue(userToken.getRequestTimes() > 0);
        Assert.assertTrue(userToken.getLastRequestTime() > 0);

//
//
        LettuceUserTokenManager tokenManager2 = new LettuceUserTokenManager("test", plus);

        Assert.assertTrue(tokenManager2.userIsLoggedIn("admin"));
        Assert.assertTrue(tokenManager2.tokenIsLoggedIn("test-token"));

        UserToken token = tokenManager2.getByToken("test-token");

        Assert.assertEquals(token.getType(), "test");
        Assert.assertEquals(token.getState(), TokenState.normal);

        tokenManager.changeTokenState("test-token", TokenState.deny);
        Thread.sleep(100);

        Assert.assertEquals(token.getState(), TokenState.deny);

        tokenManager.signOutByUserId("admin");


        Thread.sleep(100);
        Assert.assertFalse(tokenManager2.userIsLoggedIn("admin"));
        Assert.assertFalse(tokenManager2.tokenIsLoggedIn("test-token"));

    }

    @Test
    @SneakyThrows
    public void testExpire() {
        UserToken userToken = tokenManager.signIn("test-token-expire", "test", "admin", 1000);
        Assert.assertTrue(userToken.isNormal());
        Thread.sleep(1200);
        Assert.assertTrue(userToken.isExpired());
    }

    @Test
    @SneakyThrows
    public void testBenchmark() {
        long time = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(1000);

        for (int i = 0; i < 1000; i++) {
            CompletableFuture.runAsync(() -> tokenManager.signIn(IDGenerator.MD5.generate(), "test", "admin", 10_000))
                    .whenComplete((nil, err) -> latch.countDown());
        }
        latch.await(30, TimeUnit.SECONDS);
        plus.getConnection().toCompletableFuture()
                .get()
                .sync()
                .flushdb();
        System.out.println(System.currentTimeMillis() - time);
    }
}