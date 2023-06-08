package org.tooling.sqlpp.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.tooling.sqlpp.utils.pojo.StatementWithParameters;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.exc.StreamReadException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DatabindException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonQueryFileHelper {

	private static final Logger LOGGER = Logger.getLogger(JsonQueryFileHelper.class);

	private ObjectMapper mapper = new ObjectMapper();
	private String fileURL = "";
	private StatementWithParameters[] statementWithParameters = null;

	public StatementWithParameters[] getStatementWithParameters() {
		return statementWithParameters;
	}

	public JsonQueryFileHelper(String fileURL) {
		this.fileURL = fileURL;

		try {
			parseJSON(this.fileURL);
		} catch (StreamReadException e) {
			LOGGER.error(e);
		} catch (DatabindException e) {
			LOGGER.error(e);
		} catch (IOException e) {
			LOGGER.error(e);
		}
	}

	private void parseJSON(String fileURL) throws StreamReadException, DatabindException, IOException {
		InputStream is = new FileInputStream(new File(fileURL));
		this.statementWithParameters = mapper.readValue(is, StatementWithParameters[].class);
	}

}
