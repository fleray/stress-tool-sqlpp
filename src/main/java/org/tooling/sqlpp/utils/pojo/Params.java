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
	
	// used only with randomMultiInArray to specify how many elements to select randomly in given 'values' array
	@JsonProperty("quantity")
	private int quantity = 0;
	
	public Params() {
		
	}

	public Object[] getValues() {
		return values;
	}

	public String getType() {
		return type;
	}
	
	public int getQuantity() {
		return quantity;
	}
	
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

	
	@Override
	public String toString() {
		return String.format("params -> type: %s t values: %s", type, values); 
	}
}
