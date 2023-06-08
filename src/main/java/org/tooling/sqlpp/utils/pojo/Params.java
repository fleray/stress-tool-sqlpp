package org.tooling.sqlpp.utils.pojo;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties
public class Params {

	// Array of values : can be Number, Date, String, Boolean
	@JsonProperty("values")
	private Object[] values;
	
	// can be "randomInArray" or "randomInRange".
	// If values is of type String and type = randomInRange
	@JsonProperty("type")
	private String type;
	
	public Params() {
		
	}

	public Object[] getValues() {
		return values;
	}

	public String getType() {
		return type;
	}
	
	@Override
	public String toString() {
		return String.format("params -> type: %s t values: %s", type, values); 
	}
}
