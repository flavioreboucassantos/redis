package com.br.flavioreboucassantos.redisson_concurrent_sum;

import java.util.concurrent.atomic.AtomicInteger;

import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.IntegerCodec;
import org.redisson.config.Config;

public class RedissonConcurrentSum {

	/*
	 * Change Here to Select Between runThreadSafeSumTask or runNonThreadSafeSumTask
	 */
	static public final boolean isThreadSafe = false;

	/*
	 * Configurations of Concurrent Sum
	 */
	static private final int numberOfAdders = 5;
	static private final int numberOfSums = 500;
	static private final long nsTimeBetweenSums = 1000000 * 1; // 1000000 is 1 ms & * 1000 is 1 sec

	static private final AtomicInteger countAddersConcluded = new AtomicInteger(0);
	static private final AtomicInteger countRollbacks = new AtomicInteger(0);

	static private final String keyName = "valor";

	static private void print(Object p) {
		System.out.println(p);
	}

	static private RedissonClient createRedissonClientInstance() {
		final Config config = new Config();
		config.useSingleServer().setAddress("redis://127.0.0.1:6379");
		final RedissonClient redisson = Redisson.create(config);
		return redisson;
	}

	static private boolean allAddersConcluded() {
		return (countAddersConcluded.get() < numberOfAdders) ? false : true;
	}

	static public void main(String[] args) throws InterruptedException {
		RedissonClient redisson = createRedissonClientInstance();
		RBucket<Integer> bucket = redisson.getBucket(keyName, IntegerCodec.INSTANCE);
		bucket.set(0);

		for (int i = 0; i < numberOfAdders; i++) {
			Thread thread = new Thread(new RunnableConcurrentSum(createRedissonClientInstance(), keyName, numberOfSums, nsTimeBetweenSums));
			thread.start();
		}

		final long msTimeOfStart = System.currentTimeMillis();

		while (!allAddersConcluded())
			Thread.yield();

		final long msTimeToFinish = System.currentTimeMillis() - msTimeOfStart;

		final int valorEsperado = numberOfAdders * numberOfSums;
		final int valorEncontrado = Integer.valueOf(bucket.get());

		print("Tempo gasto: " + msTimeToFinish + " milliseconds");
		print("Valor esperado: " + valorEsperado);
		print("Valor encontrado: " + valorEncontrado);
		print("Rollbacks realizados: " + countRollbacks.get());

		redisson.shutdown();
	}

	static public void adderConcluded(final int _countRollbacks) {
		countRollbacks.addAndGet(_countRollbacks);
		countAddersConcluded.incrementAndGet();
	}
}
