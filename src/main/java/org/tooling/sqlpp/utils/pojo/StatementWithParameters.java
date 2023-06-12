package org.tooling.sqlpp.utils.pojo;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties
public class StatementWithParameters {

	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
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
		String query = statement;
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
//		LOGGER.debug("query: "+ query);
		return query;
	}

	private String buildParam(Params params) {
		String res = "";

		if (null == params.getType()) {
			String errMess = String.format("Params \"%s\" NEEDS to have both 'type' and 'values'", params.toString());
			LOGGER.error(errMess);
			throw new IllegalArgumentException(errMess);
		}

		Object[] values = params.getValues();
		

		if (null == values) {
			throw new NullPointerException("'values' field IS MISSING");	
		}
		else if (0 == values.length) {
			throw new IllegalArgumentException("'values' array MUST NOT be empty");	
		}
		
		switch (params.getType()) {
		case "randomInArray":
			Object randomObj = getRandomObj(values);
			res = randomObj.toString();
			break;

		case "randomMultiInArray":


			if (params.getQuantity() <= 0) {
				LOGGER.warn("Quantity MUST NOT be null: quantity defaults back to 'values.length'");
				params.setQuantity(values.length);
			}

			if (values.length == 1) {
				res = (String) values[0];
			} else if (values.length == params.getQuantity()) {
				res = "";
				for (int i = 0; i < params.getQuantity(); i++) {
					res += "," + (String) values[i];
				}
				res = res.substring(1);
			} else {
				res = "";
				for (int i = 0; i < params.getQuantity(); i++) {
					String randomElement = getRandomElement(Arrays.asList(values));
					res += "," + randomElement;
				}
				res = res.substring(1);
			}

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

	private Object getRandomObj(Object[] array) {
		int rnd = new Random().nextInt(array.length);
		return array[rnd];
	}

	private String getRandomInRange(Object[] array) throws ParseException {
		String res = "";
		if (array.length != 2) {
			String errMess = String.format("A range MUST contain only 2 values");
			LOGGER.error(errMess);
			throw new IllegalArgumentException(errMess);
		}
		if (array[0] instanceof String) {
			// range string => date MANDATORY
			LocalDate startDate = LocalDate.parse((String) array[0], DATE_FORMAT);
			LocalDate endDate = LocalDate.parse((String) array[1], DATE_FORMAT);
			res = between(startDate, endDate).toString();
		} else if (array[0] instanceof Integer) {
			int low = (int) array[0];
			int high = (int) array[1];
			res = Integer.toString((new Random().nextInt(high - low) + low));
		} else if (array[0] instanceof Double) {
			double low = (double) array[0];
			double high = (double) array[1];
			res = Double.toString((new Random().nextDouble(high - low) + low));
		}
		return res;
	}

	public String between(LocalDate startInclusive, LocalDate endExclusive) {
		long startMillis = startInclusive.toEpochDay();
		long endMillis = endExclusive.toEpochDay();
		long randomMillisSinceEpoch = 0L;

		try {
			randomMillisSinceEpoch = ThreadLocalRandom.current().nextLong(startMillis, endMillis);
		} catch (IllegalArgumentException iae) {
			LOGGER.error(iae);
		}

		return "'" + LocalDate.ofEpochDay(randomMillisSinceEpoch) + "'";
	}

	// Function select an element base on index
	// and return an element
	public String getRandomElement(List<Object> list) {
		Random rand = new Random();
		return (String) list.get(rand.nextInt(list.size()));
	}
}
