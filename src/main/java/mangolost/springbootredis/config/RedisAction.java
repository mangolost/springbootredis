package mangolost.springbootredis.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
@Service
public class RedisAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisAction.class);

    private final StringRedisTemplate stringRedisTemplate;

    @Autowired
    public RedisAction(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     *
     */
    @Scheduled(fixedDelay = 1000000000L)
    public void doAction() {

        doBatchWrite();

        doBatchWriteWithPipeLine();

        try {
            doBatchWriteWithMultiThread();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String key = "test:yyyy";
        String value = "uuuuu";
        LOGGER.info("aaaaa");
        stringRedisTemplate.opsForValue().set(key, value);
    }

    /**
     *
     */
    private void doBatchWrite() {
        long time1 = System.nanoTime();
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        for (int i = 1; i <= 1000; i++) {
            String x = "ttt:" + String.valueOf(i);
            valueOperations.set(x, x);
        }
        long time2 = System.nanoTime();
        System.out.println(time2 - time1);
    }

    /**
     *
     */
    private void doBatchWriteWithPipeLine() {
        long time1 = System.nanoTime();
        stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> redisOperations) throws DataAccessException {
                ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
                for (int i = 1; i <= 1000; i++) {
                    String x = "vvv:" + String.valueOf(i);
                    valueOperations.set(x, x);
                }
                return null;
            }
        });
        long time2 = System.nanoTime();
        System.out.println(time2 - time1);
    }

    /**
     *
     */
    private void doBatchWriteWithMultiThread() throws InterruptedException {
        int threadNum = 10;
        long time1 = System.nanoTime();
        List<List<String>> list = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            List<String> subList = new ArrayList<>();
            for (int j = 1 + 100 * i; j <= 100 * (i + 1); j++) {
                subList.add("bbb:" + j);
            }
            list.add(subList);
        }

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            int finalI = i;
            new Thread(() -> {
                ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
                System.out.println(Thread.currentThread().getName() + "准备开始写入");
                List<String> subList = list.get(finalI);
                for (String x : subList) {
                    valueOperations.set(x, x);
                }
                System.out.println(Thread.currentThread().getName() + "写入完成");
                countDownLatch.countDown();
            }, "Thread" + i).start();
        }
        countDownLatch.await();
        long time2 = System.nanoTime();
        System.out.println("写入结束");
        System.out.println(time2 - time1);
    }

}
