package com.br.flavioreboucassantos.redis.concurrent_sum;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisRunnableConcurrentSum implements Runnable {

	private final int numberOfSums;
	private final long nsTimeBetweenSums;

	final Jedis jedis;
	private final String keyName;

	/*
	 * Non Thread Safe Sum Task
	 */
	private void nonThreadSafeSumTask() throws Exception {
		int integer = Integer.valueOf(jedis.get(keyName));
		integer++;
		jedis.set(keyName, String.valueOf(integer));
	}

	private void runNonThreadSafeSumTask() {
		int numberOfSum = 0;
		try {
			// sum every nsTimeBetweenSums
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
			RedisConcurrentSum.adderConcluded(0);
		}
	}

	/*
	 * Thread Safe Sum Task
	 */
	private boolean threadSafeSumTask() throws Exception {
		/*
		 * Dica de Outra Abordagem:
		 * - Usar get dentro de transaction t.get("chave") somente quando precisar do resultado final da operação, pela sequência de realização do jedis.multi()
		 */

		// Se uma chave monitorada for alterada por outro cliente antes da execução da transação, o Redis aborta a operação, evitando conflitos de concorrência em Java.
		jedis.watch(keyName); // Monitora desde o jedis.get
		int integer = Integer.valueOf(jedis.get(keyName));
		integer++;

		/*
		 * O comando MULTI serve para iniciar uma transação.
		 * - Agrupa vários comandos em uma única etapa de execução, garantindo que todos sejam processados sequencialmente, sem a interferência de outros clientes Redis.
		 * - Atomicidade: O comando MULTI marca o início de um bloco de transação. Após o MULTI, os comandos subsequentes não são executados imediatamente, mas enfileirados.
		 * - Execução Garantida: Quando o comando EXEC é chamado, todos os comandos da fila são executados. O Redis garante que, se EXEC for chamado, todos os comandos na transação serão executados em ordem.
		 * - Cancelamento: O comando DISCARD pode ser usado para cancelar a transação, liberando a fila e descartando os comandos enfileirados.
		 * - Isolamento: Nenhuma outra solicitação de cliente será processada durante a execução da transação (entre MULTI e EXEC), o que garante a consistência dos dados.
		 */
		Transaction t = jedis.multi();
		try {
			t.set(keyName, String.valueOf(integer));

			List<Object> result = t.exec(); // Se a chave mudou, o exec() retornará null ou vazio.

			if (result == null || result.isEmpty()) // Rollback/Falha: A chave foi alterada por outro cliente.
				return false;

			// Transação concluída com sucesso!
			return true;
		} catch (Exception e) { // Captura erros de erro de lógica ou validação falha.
			// Use t.discard() para:
			// - Abortar Transações: Se você iniciou uma série de operações com MULTI e, por algum motivo (erro de lógica, validação falha), decidiu que não deve mais aplicá-las, o DISCARD garante que nada seja alterado.
			// - Limpar o Buffer da Transação: Remove da fila todos os comandos que foram enviados após o MULTI e antes do EXEC.
			// - Encerrar o modo MULTI: A conexão retorna ao estado normal, saindo do contexto de transação.
			t.discard();

			throw e; // Eliminate Thread
		} finally {
			jedis.unwatch();
		}
	}

	private void runThreadSafeSumTask() {
		int numberOfSum = 0;
		int countRollbacks = 0;
		try {
			// sum every nsTimeBetweenSums
			while (numberOfSum++ < numberOfSums) {
				if (!threadSafeSumTask()) {
					numberOfSum--;
					countRollbacks++;
				}
				final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//				System.out.println(numberOfSum + " >>> " + timeNextRun);
				while (System.nanoTime() < timeNextRun)
					Thread.yield();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RedisConcurrentSum.adderConcluded(countRollbacks);
		}
	}

	public RedisRunnableConcurrentSum(Jedis jedis, final String keyName, final int numberOfSums, final long nsTimeBetweenSums) {
		this.numberOfSums = numberOfSums;
		this.nsTimeBetweenSums = nsTimeBetweenSums;

		this.jedis = jedis;
		this.keyName = keyName;
	}

	@Override
	public void run() {
		try {

			if (RedisConcurrentSum.isThreadSafe)
				runThreadSafeSumTask();
			else
				runNonThreadSafeSumTask();

		} finally {
			jedis.close();
		}
	}
}
