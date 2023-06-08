package org.tooling.sqlpp.utils;

import java.util.concurrent.atomic.AtomicReference;

public class StateCounters {
	private AtomicReference<Long> successCounter = new AtomicReference<Long>();
	private AtomicReference<Long> errorCounter = new AtomicReference<Long>();

	public StateCounters() {
		this.successCounter.set(0L);
		this.errorCounter.set(0L);
	}

	public AtomicReference<Long> getSuccessCounter() {
		return successCounter;
	}

	public AtomicReference<Long> getErrorCounter() {
		return errorCounter;
	}
}
