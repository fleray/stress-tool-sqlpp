package org.tooling.sqlpp.utils.pojo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties
public class StatementWithParameters {

	private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyy-MM-dd");
	private static final Logger LOGGER = Logger.getLogger(StatementWithParameters.class);

	@JsonProperty("statement")
	private String statement;

	@JsonProperty("parameters")
	private List<Params> parameters;

	public String getStatement() {
		return statement;
	}

	public StatementWithParameters() {

	}

	public String buildQuery() throws IllegalArgumentException {
		String query = "";
		if (null != parameters) {
			// check number of "?" is the same as parameters size:
			int count = (int) statement.chars().filter(ch -> ch == '?').count();
			if (count != parameters.size()) {
				String errMess = String.format(
						"statement \"%s\" DOES NOT have the same number of \"?\" than size of provided parameters (%d)",
						statement, parameters.size());
				LOGGER.error(errMess);

				throw new IllegalArgumentException(errMess);
			}
			for (int i = 0; i < parameters.size(); i++) {
				query = String.format(statement.replace("?", "%s"), buildParam(parameters.get(i)));
			}
		}
		return query;
	}

	private String buildParam(Params params) {
		String res = "";
		
		if (null == params.getType()) {
			String errMess = String.format("Params \"%s\" NEEDS to have both 'type' and 'values'", params.toString());
			LOGGER.error(errMess);
			throw new IllegalArgumentException(errMess);
		}

		switch (params.getType()) {
		case "randomInArray":
			Object[] values = params.getValues();
			Object randomObj = getRandomObj(values);
			res = randomObj.toString();
			break;

		case "randomMultiInArray":
			// TODO !!
			break;

		case "randomInRange":
			try {
				res = getRandomInRange(params.getValues());
			} catch (ParseException e) {
				LOGGER.error(e);
			}
			break;

		default:
			break;
		}
		
		return res;
	}
	
	private static Object getRandomObj(Object[] array) {
	    int rnd = new Random().nextInt(array.length);
	    return array[rnd];
	}
	
	private static String getRandomInRange(Object[] array) throws ParseException {
		String res = "";
		if(array.length != 2) {
			String errMess = String.format("A range MUST contain only 2 values");
			LOGGER.error(errMess);
			throw new IllegalArgumentException(errMess);
		}
	    if(array[0] instanceof String) {
	    	// range string => date MANDATORY
	    	Date startDate = DATE_FORMATTER.parse((String) array[0]);
	    	Date endDate = DATE_FORMATTER.parse((String) array[1]);
	    	res = between(startDate, endDate).toString();
	    } else if(array[0] instanceof Integer) {
	    	int low = (int) array[0];
	    	int high = (int) array[1];
	    	res = Integer.toString((new Random().nextInt(high-low) + low));
	    }
	    return res;
	}
	
	public static String between(Date startInclusive, Date endExclusive) {
	    long startMillis = startInclusive.getTime();
	    long endMillis = endExclusive.getTime();
	    long randomMillisSinceEpoch = ThreadLocalRandom
	      .current()
	      .nextLong(startMillis, endMillis);

	    return "'" + DATE_FORMATTER.format(new Date(randomMillisSinceEpoch)) + "'";
	}
}
