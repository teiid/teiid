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

package org.teiid.dqp.internal.process;

import java.util.Arrays;
import java.util.List;

import org.teiid.dqp.internal.process.CodeTableCache;
import org.teiid.dqp.internal.process.CodeTableCache.CacheState;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.query.util.CommandContext;

/**
 */
public class TestCodeTableCache extends TestCase {

	private static CommandContext TEST_CONTEXT = new CommandContext("pid", "1", null, "test",  "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$  
	private static CommandContext TEST_CONTEXT_1 = new CommandContext("pid", "1", null, "test",  "2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	
	public TestCodeTableCache(String name) {
		super(name);
	}
	
	private static List[] exampleResultObject() {
		List record1 = Arrays.asList("US", "USA"); //$NON-NLS-1$ //$NON-NLS-2$
		List record2 = Arrays.asList("Germany", "GM"); //$NON-NLS-1$ //$NON-NLS-2$

        List[] records = new List[] { 
            record1, record2
        };
				
		return records;    
	}

	private CodeTableCache setUpSampleCodeTable(boolean setDone) {
		CodeTableCache ctc = new CodeTableCache(10);
		
		// must set the requestToCacheKeyMap first 
		int nodeId = ctc.createCacheRequest("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		List[] results = exampleResultObject();
		
		//  table/countrycode (keyElem/country, returnElem/code);
		//   r1--> 'US', 'USA'
		//   r2--> 'Germany', 'GM'
		
		try {
			ctc.loadTable(nodeId, results);
		} catch (MetaMatrixProcessingException e) {
			throw new RuntimeException(e);
		}
		if(setDone) {
			ctc.markCacheLoaded(nodeId);
		}
		return ctc;	
	}

	// Max = 1 and 1 table is set up
	private CodeTableCache setUpSampleCodeTable2() {
		CodeTableCache ctc = new CodeTableCache(1);
		
		// must set the requestToCacheKeyMap first 
		int nodeId = ctc.createCacheRequest("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List[] results = exampleResultObject();
		
		//  table/countrycode (keyElem/country, returnElem/code);
		//   r1--> 'US', 'USA'
		//   r2--> 'Germany', 'GM'
		
		try {
			ctc.loadTable(nodeId, results);
		} catch (MetaMatrixProcessingException e) {
			throw new RuntimeException(e);
		}
		ctc.markCacheLoaded(nodeId);
		return ctc;	
	}

    public void testLookupValue() throws Exception {
		CodeTableCache ctc = setUpSampleCodeTable(false);
		String code = (String) ctc.lookupValue("countrycode", "code", "country", "Germany", TEST_CONTEXT);	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		assertEquals("Actual lookup value doesn't match with expected: ", code, "GM"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** state = 0; exists*/
	public void testCacheExists1() {
		// load code table
		CodeTableCache ctc = setUpSampleCodeTable(true);

		CacheState actualState = ctc.cacheExists("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_EXISTS, actualState);	 //$NON-NLS-1$
	}

	/** state = 1; loading state */
	public void testCacheExists2() {
		CodeTableCache ctc = new CodeTableCache(10);
		
		ctc.cacheExists("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		CacheState actualState = ctc.cacheExists("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_LOADING, actualState);	 //$NON-NLS-1$
	}	

	/** state = 2; not exist */
	public void testCacheExists3() {
		CodeTableCache ctc = setUpSampleCodeTable(true);
		
		CacheState actualState = ctc.cacheExists("countrycode1", "code1", "country1", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_NOT_EXIST, actualState);	 //$NON-NLS-1$
	}	
		
	/** state = 2; not exist */
	public void testCacheExists3a() {
		CodeTableCache ctc = setUpSampleCodeTable(false);
		
		CacheState actualState = ctc.cacheExists("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_NOT_EXIST, actualState);	 //$NON-NLS-1$
	}	

	/** state = 4; overload */
	public void testCacheOverload1() {
		CodeTableCache ctc = setUpSampleCodeTable2();
		
		CacheState actualState = ctc.cacheExists("countrycode", "something", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_OVERLOAD, actualState);	 //$NON-NLS-1$
	}	

    /** test load, then clearAll, then cacheExists */
    public void testClearAllLoaded() {
        // load code table
        CodeTableCache ctc = setUpSampleCodeTable(true);

        // clear all code tables
        ctc.clearAll();

        // check state
        CacheState actualState = ctc.cacheExists("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_NOT_EXIST, actualState);   //$NON-NLS-1$
    }

    /** load table, cacheExists, clearAll, then lookupValue - this should throw an exception */
    public void testClearAllLoading() {
        // load code table
        CodeTableCache ctc = setUpSampleCodeTable(true);
        
        // check state
        CacheState actualState = ctc.cacheExists("countrycode", "code", "country", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_EXISTS, actualState);  //$NON-NLS-1$

        // clear all code tables before it can be read
        ctc.clearAll();

        // lookup a value - this should throw an exception
        try {
            ctc.lookupValue("countrycode", "code", "country", "US", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            fail("Expected exception during lookup"); //$NON-NLS-1$
        } catch(MetaMatrixComponentException e) {
            // expected this
        }
    }
    
    public void testVdbSpecificCaching() {
		// load code table
		CodeTableCache ctc = setUpSampleCodeTable(true);

		CacheState actualState = ctc.cacheExists("countrycode", "code", "country", TEST_CONTEXT_1); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("Actual cache state doesn't match with expected: ", CacheState.CACHE_NOT_EXIST, actualState);	 //$NON-NLS-1$
    }
    
    public void testDuplicateKeyException() {
    	CodeTableCache ctc = new CodeTableCache(1);
		
		// must set the requestToCacheKeyMap first 
		int nodeId = ctc.createCacheRequest("table", "key", "value", TEST_CONTEXT); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List[] results = new List[] {
        		Arrays.asList(1, 2),
        		Arrays.asList(1, 3),
        }; 
		
		try {
			ctc.loadTable(nodeId, results);
			fail("expected exception"); //$NON-NLS-1$
		} catch (MetaMatrixProcessingException e) {
			assertEquals("Duplicate code table 'table' key 'value' value '1'", e.getMessage()); //$NON-NLS-1$
		}
    }

}
