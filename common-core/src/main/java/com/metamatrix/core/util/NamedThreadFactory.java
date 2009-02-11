package com.metamatrix.core.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
	
	private AtomicInteger threadNumber = new AtomicInteger();
	private String threadBaseName;
	
	public NamedThreadFactory(String name) {
		this.threadBaseName = (name != null ? name : "Worker_"); //$NON-NLS-1$ 
	}

	public Thread newThread(Runnable r) {
		String threadName = threadBaseName + threadNumber.getAndIncrement();
		Thread t = new Thread(r, threadName);
		t.setDaemon(true);
		return t;
	}
}


