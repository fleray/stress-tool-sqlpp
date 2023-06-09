package org.tooling.sqlpp;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.tooling.sqlpp.utils.StateCounters;
import org.tooling.sqlpp.utils.pojo.StatementWithParameters;

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.google.common.util.concurrent.RateLimiter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CouchbaseSQLPP {

	private static final Logger LOGGER = Logger.getLogger(CouchbaseSQLPP.class);

	private ReactiveCluster reactiveCluster;
	private AtomicReference<Long> counter = new AtomicReference<Long>();
	private static StateCounters COUNTERS = new StateCounters();

	private Instant startTime;

	private StatementWithParameters[] statementWithParameters;

	private long nbQPSWanted;

	public CouchbaseSQLPP(ReactiveCluster reactiveCluster, StatementWithParameters[] statementWithParameters,
			long nbQPSWanted) {
		this.reactiveCluster = reactiveCluster;
		this.statementWithParameters = statementWithParameters;
		this.nbQPSWanted = nbQPSWanted;
	}

	private StatementWithParameters getRandomStatementWithParameters() {
		int rnd = new Random().nextInt(this.statementWithParameters.length);
		return this.statementWithParameters[rnd];
	}

	/**
	 * Generate random SQL++ queries taking into account the provided file.
	 * 
	 * @param numQueries
	 * @param numTasks
	 * @param numThreads
	 * @param start
	 * @param end
	 * @param logAfter
	 */
	public StateCounters generateRandomSQLQueries(long numOperations, int numTasks, int numThreads, Long logAfter) {
		counter.set(0L); // Counter for number of operations
		startTime = Instant.now(); // Start time for measuring duration
		AtomicReference<Long> errorCounter = COUNTERS.getErrorCounter();

		LOGGER.debug("numOperations : " + numOperations);

//		String query = "SELECT COUNT(*) AS count FROM `doctolib_bucket`.`doctolib_scope`.`collection5` WHERE event IS NOT MISSING";
//		String query = "SELECT COUNT(*) AS count FROM `travel-sample`.`_default`.`_default` WHERE type IS NOT MISSING";
		QueryOptions qo = QueryOptions.queryOptions().adhoc(false);
		qo.retryStrategy(BestEffortRetryStrategy.INSTANCE);

		if (nbQPSWanted > 0) {
			RateLimiter limiter = RateLimiter.create(nbQPSWanted);
			stressCluster(numOperations, numTasks, numThreads, logAfter, errorCounter, qo, limiter);
		} else {
			stressCluster(numOperations, numTasks, numThreads, logAfter, errorCounter, qo, null);
		}

		return COUNTERS;

	}

	private void stressCluster(long numOperations, int numTasks, int numThreads, Long logAfter,
			AtomicReference<Long> errorCounter, QueryOptions qo, RateLimiter limiter) {
		Flux.range(0, numTasks)
			.parallel()
			.runOn(Schedulers.newParallel("se-sql-stresstool", numThreads))
				.flatMap(i -> Flux.range(0, (int) numOperations).flatMap(j -> runQuery(qo, limiter))
						.onErrorContinue((throwable, obj) -> {
							// Depends what we want to happen - here silently swallowing any errors
							errorCounter.accumulateAndGet(1L, Long::sum);
					LOGGER.debug("==> errorCounter = " + errorCounter.get() + "Error is: " + throwable +  " \t thread"
							+ Thread.currentThread().getName());
						}))
//			.runOn(Schedulers.newParallel("se-sql-result", numThreads))
			.doOnNext(result -> handleResult(logAfter, result)).sequential().blockLast();
	}

	private Mono<ReactiveQueryResult> runQuery(QueryOptions qo, RateLimiter limiter) {

		if (limiter != null) {
			limiter.acquire();
		}
		String query = getRandomStatementWithParameters().buildQuery();
		
		LOGGER.debug("query= " + query);

		return reactiveCluster.query(query, qo);
	}

	private void handleResult(Long logAfter, ReactiveQueryResult result) {
		AtomicReference<Long> successCounter = COUNTERS.getSuccessCounter();
		successCounter.accumulateAndGet(1L, Long::sum);

		long cnt = counter.accumulateAndGet(1L, Long::sum);
		if (cnt % logAfter == 0) {
			Instant endTime = Instant.now();
			long durationMillis = Duration.between(startTime, endTime).toMillis();
			double qps = ((cnt * 1.0) / durationMillis) * 1000.0;
			int roundQps = (int) Math.round(qps);
			LOGGER.info(" QUERIES/SEC: " + roundQps + "\t cnt = " + cnt + "\t durationMillis = " + durationMillis);
			LOGGER.info(" ERRORS: " + COUNTERS.getErrorCounter());
			startTime = Instant.now(); // Reset start time for the next batch
			counter.set(0L);

		}
//		LOGGER.debug("accumuCounter = " + accumuCounter.get() + " \t thread" + Thread.currentThread().getName());
	}

}
