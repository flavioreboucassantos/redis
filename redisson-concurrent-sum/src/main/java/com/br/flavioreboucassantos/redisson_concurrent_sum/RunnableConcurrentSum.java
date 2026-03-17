package com.br.flavioreboucassantos.redisson_concurrent_sum;

import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.IntegerCodec;

public class RunnableConcurrentSum implements Runnable {

	private final int numberOfSums;
	private final long nsTimeBetweenSums;

	final RedissonClient redisson;
	final String lockName;
	final RBucket<Integer> bucket;

	/*
	 * Non Thread Safe Sum Task
	 */
	private void nonThreadSafeSumTask() {
		int integer = bucket.get();
		integer++;
		bucket.set(integer);
	}

	private void runNonThreadSafeSumTask() {
		// sum every nsTimeBetweenSums
		int numberOfSum = 0;
		while (numberOfSum++ < numberOfSums) {
			nonThreadSafeSumTask();
			final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//			System.out.println(numberOfSum + " >>> " + timeNextRun);
			while (System.nanoTime() < timeNextRun)
				Thread.yield();
		}
		RedissonConcurrentSum.adderConcluded(0);
	}

	/*
	 * Thread Safe Sum Task
	 */
	private void threadSafeSumTask() {
		final RLock lock = redisson.getLock(lockName); // lockName concorre com chaves no Redis

		try {
			/*
			 * - Não realiza rollbacks.
			 * - Não executa releitura da variável remota.
			 * - Não sobrecarrega o processador do microserviço.
			 * - O atraso para alcançar allAddersConcluded é devido a troca de dados para manutenção da Lock Remota em cada chamada local.
			 */
			lock.lock();

			int integer = bucket.get();

			integer++;

			bucket.set(integer);

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			if (lock.isHeldByCurrentThread())
				lock.unlock();
		}
	}

	private void runThreadSafeSumTask() {
		// sum every nsTimeBetweenSums
		int numberOfSum = 0;
		int countRollbacks = 0;
		while (numberOfSum++ < numberOfSums) {
			threadSafeSumTask();
			final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//			System.out.println(numberOfSum + " >>> " + timeNextRun);
			while (System.nanoTime() < timeNextRun)
				Thread.yield();
		}
		RedissonConcurrentSum.adderConcluded(countRollbacks);
	}

	public RunnableConcurrentSum(RedissonClient redisson, final String keyName, final int numberOfSums, final long nsTimeBetweenSums) {
		this.numberOfSums = numberOfSums;
		this.nsTimeBetweenSums = nsTimeBetweenSums;

		this.redisson = redisson;
		this.lockName = "_" + keyName;
		bucket = redisson.getBucket(keyName, IntegerCodec.INSTANCE);
	}

	@Override
	public void run() {
		try {

			if (RedissonConcurrentSum.isThreadSafe)
				runThreadSafeSumTask();
			else
				runNonThreadSafeSumTask();

		} finally {
			redisson.shutdown();
		}
	}
}
