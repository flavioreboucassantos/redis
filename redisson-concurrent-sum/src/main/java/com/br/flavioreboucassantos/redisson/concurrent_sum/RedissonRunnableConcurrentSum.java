package com.br.flavioreboucassantos.redisson.concurrent_sum;

import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.IntegerCodec;

public class RedissonRunnableConcurrentSum implements Runnable {

	private final int numberOfSums;
	private final long nsTimeBetweenSums;

	final RedissonClient redisson;
	final RLock lock;
	final RBucket<Integer> bucket;

	/*
	 * Non Thread Safe Sum Task
	 */
	private void nonThreadSafeSumTask() throws Exception {
		int integer = bucket.get();
		integer++;
		bucket.set(integer);
	}

	private void runNonThreadSafeSumTask() {
		try {
			// sum every nsTimeBetweenSums
			int numberOfSum = 0;
			while (numberOfSum++ < numberOfSums) {
				nonThreadSafeSumTask();
				final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//				System.out.println(numberOfSum + " >>> " + timeNextRun);
				while (System.nanoTime() < timeNextRun)
					Thread.yield();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RedissonConcurrentSum.adderConcluded(0);
		}
	}

	/*
	 * Thread Safe Sum Task
	 */
	private void threadSafeSumTask() throws Exception {
		try {
			/*
			 * - Não realiza rollbacks.
			 * - Não executa releitura da variável remota.
			 * - Não sobrecarrega o processador do microserviço.
			 * - O atraso para alcançar allAddersConcluded é devido a troca de dados para utilização da Lock Remota em cada chamada local.
			 */
			lock.lock();

			int integer = bucket.get();

			integer++;

			bucket.set(integer);

		} catch (Exception e) {
			throw e;

		} finally {
			if (lock.isHeldByCurrentThread())
				lock.unlock();
		}
	}

	private void runThreadSafeSumTask() {
		int numberOfSum = 0;
		int countRollbacks = 0;
		try {
			// sum every nsTimeBetweenSums
			while (numberOfSum++ < numberOfSums) {
				threadSafeSumTask();
				final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//				System.out.println(numberOfSum + " >>> " + timeNextRun);
				while (System.nanoTime() < timeNextRun)
					Thread.yield();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RedissonConcurrentSum.adderConcluded(countRollbacks);
		}
	}

	public RedissonRunnableConcurrentSum(RedissonClient redisson, final String keyName, final int numberOfSums, final long nsTimeBetweenSums) {
		this.numberOfSums = numberOfSums;
		this.nsTimeBetweenSums = nsTimeBetweenSums;

		this.redisson = redisson;
		bucket = redisson.getBucket(keyName, IntegerCodec.INSTANCE);
		lock = redisson.getLock("_" + keyName); // lockName concorre com chaves no Redis
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
