/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.handler.timeout.TimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.metadata.TransformationMetadata.Resource;

@SuppressWarnings("nls")
public class TestLocalConnections {
	
	private final class SimpleUncaughtExceptionHandler implements
			UncaughtExceptionHandler {
		Throwable t;

		@Override
		public void uncaughtException(Thread arg0, Throwable arg1) {
			t = arg1;
		}
	}

	static ReentrantLock lock = new ReentrantLock();
	static Condition waiting = lock.newCondition();
	static Condition wait = lock.newCondition();
	
	public static int blocking() throws InterruptedException {
		lock.lock();
		try {
			waiting.signal();
			if (!wait.await(2, TimeUnit.SECONDS)) {
				throw new TimeoutException();
			}
		} finally {
			lock.unlock();
		}
		return 1;
	}

	static FakeServer server = new FakeServer();
	
	@BeforeClass public static void oneTimeSetup() {
    	server.setUseCallingThread(true);
    	MetadataStore ms = new MetadataStore();
    	Schema s = new Schema();
    	s.setName("test");
    	FunctionMethod function = new FunctionMethod("foo", null, FunctionCategoryConstants.MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, TestLocalConnections.class.getName(), "blocking", new FunctionParameter[0], new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER), true, FunctionMethod.Determinism.NONDETERMINISTIC);
    	s.addFunction(function);
    	ms.addSchema(s);
    	server.deployVDB("test", ms, new LinkedHashMap<String, Resource>());
	}
	
	@AfterClass public static void oneTimeTearDown() {
		server.stop();
	}
	
	@Test public void testConcurrentExection() throws Throwable {
    	
    	Thread t = new Thread() {
    		
    		public void run() {
    			try {
	    	    	Connection c = server.createConnection("jdbc:teiid:test");
	    	    	
	    	    	Statement s = c.createStatement();
	    	    	s.execute("select foo()");
    			} catch (Exception e) {
    				throw new RuntimeException(e);
    			}
    		}
    	};
    	SimpleUncaughtExceptionHandler handler = new SimpleUncaughtExceptionHandler();
    	t.setUncaughtExceptionHandler(handler);
    	t.start();
    	
    	lock.lock();
    	try {
    		waiting.await();
    	} finally {
    		lock.unlock();
    	}
    	Connection c = server.createConnection("jdbc:teiid:test");
    	Statement s = c.createStatement();
    	s.execute("select * from tables");
    	
    	lock.lock();
    	try {
    		wait.signal();
    	} finally {
    		lock.unlock();
		}
    	t.join(2000);
    	if (t.isAlive()) {
    		fail();
    	}
    	if (handler.t != null) {
    		throw handler.t;
    	}
	}
	
}
