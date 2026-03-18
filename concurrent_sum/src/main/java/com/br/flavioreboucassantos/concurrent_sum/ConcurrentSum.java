package com.br.flavioreboucassantos.concurrent_sum;

import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;

public class ConcurrentSum {

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

	static private Jedis getNewJedisConnection() {
		return new Jedis("localhost", 6379);
	}

	static private boolean allAddersConcluded() {
		return (countAddersConcluded.get() < numberOfAdders) ? false : true;
	}

	static public void main(String[] args) throws InterruptedException {
		Jedis jedis = getNewJedisConnection();
		jedis.set(keyName, "0");
		jedis.close();

		for (int i = 0; i < numberOfAdders; i++) {
			Thread thread = new Thread(new RunnableConcurrentSum(getNewJedisConnection(), keyName, numberOfSums, nsTimeBetweenSums));
			thread.start();
		}

		final long msTimeOfStart = System.currentTimeMillis();

		while (!allAddersConcluded())
			Thread.yield();

		final long msTimeToFinish = System.currentTimeMillis() - msTimeOfStart;

		final int valorEsperado = numberOfAdders * numberOfSums;
		final int valorEncontrado = Integer.valueOf(jedis.get(keyName));

		print("Tempo gasto: " + msTimeToFinish + " milliseconds");
		print("Valor esperado: " + valorEsperado);
		print("Valor encontrado: " + valorEncontrado);
		print("Rollbacks realizados: " + countRollbacks.get());
	}

	static public void adderConcluded(final int _countRollbacks) {
		countRollbacks.addAndGet(_countRollbacks);
		countAddersConcluded.incrementAndGet();
	}
}
