/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.process;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.query.util.CommandContext;

/**
 */
public class TestCodeTableCache extends TestCase {

	public TestCodeTableCache(String name) {
		super(name);
	}
	
	private static List[] exampleResultObject() { 
		List record1 = new ArrayList();
		record1.add("US"); //$NON-NLS-1$
		record1.add("USA"); //$NON-NLS-1$
	
		List record2 = new ArrayList();
		record2.add("Germany"); //$NON-NLS-1$
		record2.add("GM"); //$NON-NLS-1$

        List[] records = new List[] { 
            record1, record2
        };
				
		return records;    
	}

	private CodeTableCache setUpSampleCodeTable(boolean setDone) {
		CodeTableCache ctc = new CodeTableCache(10);
		
		// must set the requestToCacheKeyMap first 
		int nodeId = ctc.createCacheRequest("countrycode", "code", "country", new RequestID(0)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		List[] results = exampleResultObject();
		
		//  table/countrycode (keyElem/country, returnElem/code);
		//   r1--> 'US', 'USA'
		//   r2--> 'Germany', 'GM'
		
        RequestID rID = new RequestID(0);
		ctc.loadTable(rID, nodeId, results);
		if(setDone) {
			ctc.markCacheLoaded(rID, nodeId);
		}
		return ctc;	
	}

	// Max = 1 and 1 table is set up
	private CodeTableCache setUpSampleCodeTable2() {
		CodeTableCache ctc = new CodeTableCache(1);
		
		// must set the requestToCacheKeyMap first 
		int nodeId = ctc.createCacheRequest("countrycode", "code", "country", new RequestID(0)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List[] results = exampleResultObject();
		
		//  table/countrycode (keyElem/country, returnElem/code);
		//   r1--> 'US', 'USA'
		//   r2--> 'Germany', 'GM'
		
        RequestID rID = new RequestID(0);
		ctc.loadTable(rID, nodeId, results);
		ctc.markCacheLoaded(rID, nodeId);
		return ctc;	
	}

    public void testIsCodeTableResponse() {
        CodeTableCache ctc = setUpSampleCodeTable(false);
        boolean isCTR = ctc.isCodeTableResponse(new RequestID(0), -1);
        assertTrue("Actual isCodeTableResponse value doesn't match with expected: ", isCTR); //$NON-NLS-1$
    }
    
    public void testLookupValue() throws Exception {
		CodeTableCache ctc = setUpSampleCodeTable(false);
		String code = (String) ctc.lookupValue("countrycode", "code", "country", "Germany");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		assertEquals("Actual lookup value doesn't match with expected: ", code, "GM"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** state = 0; exists*/
	public void testCacheExists1() {
		// load code table
		CodeTableCache ctc = setUpSampleCodeTable(true);
        CommandContext context = new CommandContext("pid", "countrycode", null, 10,  null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$

		int actualState = ctc.cacheExists("countrycode", "code", "country", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CodeTableCache.CACHE_EXISTS, actualState);	 //$NON-NLS-1$
	}

	/** state = 1; loading state */
	public void testCacheExists2() {
		CodeTableCache ctc = new CodeTableCache(10);
        CommandContext context = new CommandContext("pid", "countrycode", null, 10,  null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$
		
		ctc.cacheExists("countrycode", "code", "country", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		int actualState = ctc.cacheExists("countrycode", "code", "country", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CodeTableCache.CACHE_LOADING, actualState);	 //$NON-NLS-1$
	}	

	/** state = 2; not exist */
	public void testCacheExists3() {
		CodeTableCache ctc = setUpSampleCodeTable(true);
        CommandContext  context = new CommandContext ("pid1", "countrycode1", null, 5,  null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$
		
		int actualState = ctc.cacheExists("countrycode1", "code1", "country1", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CodeTableCache.CACHE_NOT_EXIST, actualState);	 //$NON-NLS-1$
	}	
		
	/** state = 2; not exist */
	public void testCacheExists3a() {
		CodeTableCache ctc = setUpSampleCodeTable(false);
        CommandContext context = new CommandContext("pid", "countrycode", null, 5,  null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$
		
		int actualState = ctc.cacheExists("countrycode", "code", "country", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CodeTableCache.CACHE_NOT_EXIST, actualState);	 //$NON-NLS-1$
	}	

	/** state = 4; overload */
	public void testCacheOverload1() {
		CodeTableCache ctc = setUpSampleCodeTable2();
        CommandContext context = new CommandContext("pid", "countrycode", null, 5,  null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$
		
		int actualState = ctc.cacheExists("countrycode", "something", "country", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CodeTableCache.CACHE_OVERLOAD, actualState);	 //$NON-NLS-1$
	}	

    /** test load, then clearAll, then cacheExists */
    public void testClearAllLoaded() {
        // load code table
        CodeTableCache ctc = setUpSampleCodeTable(true);
        CommandContext context = new CommandContext("pid", "countrycode", null, 5,  null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$

        // clear all code tables
        ctc.clearAll();

        // check state
        int actualState = ctc.cacheExists("countrycode", "code", "country", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("Actual cache state doesn't match with expected: ", CodeTableCache.CACHE_NOT_EXIST, actualState);   //$NON-NLS-1$
    }

    /** load table, cacheExists, clearAll, then lookupValue - this should throw an exception */
    public void testClearAllLoading() {
        // load code table
        CodeTableCache ctc = setUpSampleCodeTable(true);
        CommandContext context = new CommandContext("pid", "countrycode", null, 5,  null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$

        // check state
        int actualState = ctc.cacheExists("countrycode", "code", "country", context); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("Actual cache state doesn't match with expected: ", CodeTableCache.CACHE_EXISTS, actualState);  //$NON-NLS-1$

        // clear all code tables before it can be read
        ctc.clearAll();

        // lookup a value - this should throw an exception
        try {
            ctc.lookupValue("countrycode", "code", "country", "US"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            fail("Expected exception during lookup"); //$NON-NLS-1$
        } catch(MetaMatrixComponentException e) {
            // expected this
        }
    }

}
