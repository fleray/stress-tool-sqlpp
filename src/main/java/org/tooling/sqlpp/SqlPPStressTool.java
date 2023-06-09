package org.tooling.sqlpp;

import java.time.Duration;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.tooling.sqlpp.utils.JsonQueryFileHelper;
import org.tooling.sqlpp.utils.StateCounters;
import org.tooling.sqlpp.utils.pojo.StatementWithParameters;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.error.PlanningFailureException;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;

public class SqlPPStressTool {

	private static final Logger LOGGER = Logger.getLogger(SqlPPStressTool.class);

	public static void main(String[] args) {

		/* parameters to use if no command line is found */
		boolean precheckQueries = false;
		String username = "Administrator";
		String password = "password";
		String host = "XXXXXXX.amazonaws.com";
//		String host = "localhost";
		String fileURL = "";
		long nbQPSWanted = -1L;

		/* Read request */
		long numQueries = 999999999L;
		int numTasks = 5;
		int numThreads = Runtime.getRuntime().availableProcessors();
		Long logAfter = 1000L;

		CommandLine commandLine;
		Option option_h = Option.builder("h").argName("host").hasArg().desc("couchbase ip").build();
		Option option_u = Option.builder("u").argName("username").hasArg().desc("couchbase username").build();
		Option option_p = Option.builder("p").argName("password").hasArg().desc("couchbase password").build();
		Option option_f = Option.builder("f").argName("file").hasArg().desc("JSON file containing SQL++ queries")
				.build();
		Option option_pc = Option.builder("pc").argName("pre-check").hasArg(false).desc("Pre-check all SQL++ queries")
				.build();
		Option option_queries_limit = Option.builder("qps").argName("query-per-second").hasArg()
				.desc("Approx. QPS (queries per second) desired").build();
		Option option_number_queries = Option.builder("n").argName("queries-number").hasArg()
				.desc("Total number of queries to run (-1: no limit)").build();
		Option option_number_threads = Option.builder("t").argName("threads").hasArg()
				.desc("Total number of threads to use: default (and max) value is max # cores available on the machine").build();

		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		options.addOption(option_h);
		options.addOption(option_u);
		options.addOption(option_p);
		options.addOption(option_f);
		options.addOption(option_pc);
		options.addOption(option_queries_limit);
		options.addOption(option_number_queries);
		options.addOption(option_number_threads);

		String header = "               [<arg1> [<arg2> [<arg3> ...\n       Options, flags and arguments may be in any order";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("CLIsample", header, options, null, true);

		try {
			commandLine = parser.parse(options, args);

			LOGGER.info("commandLine = " + commandLine.toString());

			if (commandLine.hasOption("h")) {
				LOGGER.debug(String.format("host ip: %s%n", commandLine.getOptionValue("h")));
				host = commandLine.getOptionValue("h");
			}

			if (commandLine.hasOption("u")) {
				LOGGER.debug(String.format("couchbase username: %s%n", commandLine.getOptionValue("u")));
				username = commandLine.getOptionValue("u");
			}

			if (commandLine.hasOption("p")) {
				LOGGER.debug(String.format("couchbase password: %s%n", commandLine.getOptionValue("p")));
				password = commandLine.getOptionValue("p");
			}

			if (commandLine.hasOption("f")) {
				LOGGER.debug(String.format("JSON queries file: %s%n", commandLine.getOptionValue("f")));
				fileURL = commandLine.getOptionValue("f");
			}

			if (commandLine.hasOption("pc")) {
				LOGGER.debug(String.format("Pre-check for all queries ENABLED%n"));
				precheckQueries = true;
			}

			if (commandLine.hasOption("qps")) {
				LOGGER.debug(String.format("Throughput desired (QPS): %s%n", commandLine.getOptionValue("qps")));
				nbQPSWanted = Long.parseLong(commandLine.getOptionValue("qps"));
			}

			if (commandLine.hasOption("n")) {
				LOGGER.debug(String.format("Total number of queries desired: %s%n", commandLine.getOptionValue("n")));
				long value = Long.parseLong(commandLine.getOptionValue("n"));
				if (value > 0) {
					numQueries = value;
				}
			}

			if (commandLine.hasOption("t")) {
				numThreads = Math.min(Runtime.getRuntime().availableProcessors(),
						Integer.parseInt(commandLine.getOptionValue("t")));
				LOGGER.debug(String.format("Total number of threads to use: %s%n", commandLine.getOptionValue("t")));
			}

		} catch (ParseException exception) {
			LOGGER.error("Parse error: " + exception);
		}

		JsonQueryFileHelper jsonQueryFileHelper = new JsonQueryFileHelper(fileURL);
		StatementWithParameters[] statementWithParameters = jsonQueryFileHelper.getStatementWithParameters();

		if (null == statementWithParameters) {
			LOGGER.error(String.format("Invalid SQL++ query file: re-check no '\"' is missing in %s! Script STOPS now.",
					fileURL));
			return;
		}

		LOGGER.info(String.format("---- Script STARTED at %s ----", new Date().toString()));

		if (precheckQueries) {
			if (!validateAllQueries(host, username, password, statementWithParameters)) {
				LOGGER.error("At least 1 query has not been validated! Script STOPS now.");
				return;
			}
		} else {
			LOGGER.info("Pre-check DISABLED: no pre-check for each query in queries file");
		}

		try (Cluster cluster = Cluster.connect(host,
				ClusterOptions.clusterOptions(username, password).environment(env -> {
					env.securityConfig(
							SecurityConfig.enableTls(false).trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
							.retryStrategy(BestEffortRetryStrategy.withExponentialBackoff(Duration.ofNanos(1000),
									Duration.ofMillis(1), 2));
					// Customize client settings by calling methods on the "env" variable.
				}))) {

			CouchbaseSQLPP sqlPlusPlus = new CouchbaseSQLPP(cluster.reactive(), statementWithParameters, nbQPSWanted);

			/* Queries */
			if (numQueries > 0) {
				LOGGER.info("numQueries = " + numQueries);
				LOGGER.info("numTasks = " + numTasks);
				LOGGER.info("numThreads = " + numThreads);
				sqlPlusPlus.generateRandomSQLQueries(numQueries / (numTasks), numTasks, numThreads, logAfter);
				StateCounters counter = sqlPlusPlus.generateRandomSQLQueries(numQueries % numTasks, 1, 1, logAfter);

				LOGGER.info("=================");
				LOGGER.info("NUM QUERIES REQUESTED: \t " + numQueries);
				LOGGER.info(" + TOTAL SUCCESS: \t " + counter.getSuccessCounter());
				LOGGER.info(" + TOTAL FAILURE: \t" + counter.getErrorCounter());
				LOGGER.info("=================");
			}

		}

		LOGGER.info(String.format("---- Script ENDED at %s ----", new Date().toString()));
		System.exit(0);
	}

	private static boolean validateAllQueries(String host, String username, String password,
			StatementWithParameters[] statementWithParameters) {
		boolean result = true;

		try (Cluster cluster = Cluster.connect(host,
				ClusterOptions.clusterOptions(username, password).environment(env -> {
					env.securityConfig(
							SecurityConfig.enableTls(false).trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
							.retryStrategy(BestEffortRetryStrategy.withExponentialBackoff(Duration.ofNanos(1000),
									Duration.ofMillis(1), 2));
					// Customize client settings by calling methods on the "env" variable.
				}))) {

			for (StatementWithParameters swp : statementWithParameters) {
				String queryString = swp.buildQuery();
				LOGGER.debug("queryString: " + queryString);

				QueryResult query = cluster.query(queryString);

				List<JsonObject> rowsAsObject = query.rowsAsObject();
				for (JsonObject obj : rowsAsObject) {
					LOGGER.debug(obj);
				}
			}
		} catch (IndexFailureException | PlanningFailureException | IllegalArgumentException ex) {
			LOGGER.error(ex);
			result = false;
		}
		return result;

	}

}