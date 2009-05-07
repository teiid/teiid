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

package org.teiid.dqp.internal.cache;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.metamatrix.cache.FakeCache.FakeCacheFactory;
import com.metamatrix.common.buffer.impl.SizeUtility;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class TestResultSetCache {
    
    @Test public void testSetAndGetResultsForSession() throws Exception{
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_CONN);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	List[] result1 = new List[]{new ArrayList()};
    	cache.setResults(id1, new CacheResults(result1, 1, true ), "req1");  //$NON-NLS-1$
    	CacheID id2 = new CacheID("12346", "select * from table2");  //$NON-NLS-1$//$NON-NLS-2$
    	List[] result2 = new List[]{new ArrayList()};
    	cache.setResults(id2, new CacheResults(result2, 1, true), "req2" );  //$NON-NLS-1$
    	assertEquals(result1, cache.getResults(id1, new int[]{1, 500}).getResults()); 
    	assertEquals(result2, cache.getResults(id2, new int[]{1, 500}).getResults()); 
    }
    
    @Test public void testSetAndGetResultsForVDB() throws Exception{
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_VDB);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("vdb1", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	List[] result1 = new List[]{new ArrayList()};
    	cache.setResults(id1, new CacheResults(result1, 1, true) , "req1");  //$NON-NLS-1$
    	CacheID id2 = new CacheID("vdb2", "select * from table2");  //$NON-NLS-1$//$NON-NLS-2$
    	List[] result2 = new List[]{new ArrayList()};
    	cache.setResults(id2, new CacheResults(result2, 1, true ), "req2");  //$NON-NLS-1$
    	assertEquals(cache.getResults(id1, new int[]{1, 500}).getResults(), result1); 
    	assertEquals(cache.getResults(id2, new int[]{1, 500}).getResults(), result2); 
    }
    
//    @Test public void testRemoveConnection() throws Exception{
//    	Properties props = new Properties();
//    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
//    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
//    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_CONN);
//    	ResultSetCache cache = new ResultSetCache(props);
//    	CacheID id1 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
//    	List[] result1 = new List[]{new ArrayList()};
//    	cache.setResults(id1, new CacheResults(result1, 0, true ));  //$NON-NLS-1$//$NON-NLS-2$
//    	CacheID id2 = new CacheID("12346",  "select * from table2");  //$NON-NLS-1$//$NON-NLS-2$
//    	List[] result2 = new List[]{new ArrayList()};
//    	cache.setResults(id2, new CacheResults(result2, 0, true) );  //$NON-NLS-1$//$NON-NLS-2$
//    	cache.removeConnection(id1.getScopeID());
//    	assertNull(cache.getResults(id1, new int[]{0, 500})); //$NON-NLS-1$
//    }
//    
//    @Test public void testRemoveVDB() throws Exception{
//    	Properties props = new Properties();
//    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
//    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
//    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_VDB);
//    	ResultSetCache cache = new ResultSetCache(props);
//    	CacheID id1 = new CacheID("vdb1", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
//    	List[] result1 = new List[]{new ArrayList()};
//    	cache.setResults(id1, new CacheResults(result1, 0, true ));  //$NON-NLS-1$//$NON-NLS-2$
//    	CacheID id2 = new CacheID("vdb2", "select * from table2");  //$NON-NLS-1$//$NON-NLS-2$
//    	List[] result2 = new List[]{new ArrayList()};
//    	cache.setResults(id2, new CacheResults(result2, 0, true ));  //$NON-NLS-1$//$NON-NLS-2$
//    	cache.removeVDB(id1.getScopeID());
//    	assertNull(cache.getResults(id1, new int[]{0, 500})); //$NON-NLS-1$
//    }
    
    @Test public void testClearAllCache() throws Exception{
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_VDB);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("vdb1", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	List[] result1 = new List[]{new ArrayList()};
    	cache.setResults(id1, new CacheResults(result1, 1, true), "req1" );  //$NON-NLS-1$
    	CacheID id2 = new CacheID("vdb2", "select * from table2");  //$NON-NLS-1$//$NON-NLS-2$
    	List[] result2 = new List[]{new ArrayList()};
    	cache.setResults(id2, new CacheResults(result2, 1, true ), "req2");  //$NON-NLS-1$
    	cache.clear();
    	assertNull(cache.getResults(id1, new int[]{1, 500})); 
    }
    
    @Test public void testSetAndGetResultsForSession1() throws Exception{
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_CONN);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	List row1 = new ArrayList();
    	row1.add("1"); //$NON-NLS-1$
    	List row2 = new ArrayList();
    	row2.add("2"); //$NON-NLS-1$
    	List row3 = new ArrayList();
    	row3.add("3"); //$NON-NLS-1$
    	List row4 = new ArrayList();
    	row4.add("4"); //$NON-NLS-1$
    	List[] result1 = new List[]{row1, row2, row3, row4};
    	cache.setResults(id1, new CacheResults(result1, 1, true ), "req1");   //$NON-NLS-1$
    	assertEquals(cache.getResults(id1, new int[]{1, 500}).getResults(), result1); 
    	assertEquals(cache.getResults(id1, new int[]{1, 2}).getResults()[0], row1);
    }
    
    @Test public void testSetAndGetResultsForSession2() throws Exception{
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_CONN);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	List row1 = new ArrayList();
    	row1.add("1"); //$NON-NLS-1$
    	List row2 = new ArrayList();
    	row2.add("2"); //$NON-NLS-1$
    	List row3 = new ArrayList();
    	row3.add("3"); //$NON-NLS-1$
    	List row4 = new ArrayList();
    	row4.add("4"); //$NON-NLS-1$
    	List[] result1 = new List[]{row1, row2, row3, row4};
    	cache.setResults(id1, new CacheResults(result1, 1, false) , "req1");   //$NON-NLS-1$
    	List row5 = new ArrayList();
    	row5.add("5"); //$NON-NLS-1$
    	List[] result2 = new List[]{row5};
    	cache.setResults(id1, new CacheResults(result2, 5, true), "req1" );  //$NON-NLS-1$
    	assertEquals(cache.getResults(id1, new int[]{1, 500}).getResults()[4], row5); 
    	assertEquals(cache.getResults(id1, new int[]{1, 2}).getResults()[0], row1);
    }
    
    @Test public void testMaxSize() throws Exception{
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "1"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_CONN);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("vdb1", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheResults result1 = createResults(500000, 1, 1, true);
    	CacheID id2 = new CacheID("vdb2", "select * from table2");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheResults result2 = createResults(500000, 1, 1, true);
    	CacheID id3 = new CacheID("vdb1", "select * from table3");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheResults result3 = createResults(500000, 1, 1, true);
    	//add two results
    	cache.setResults(id1, result1, "req1" );   //$NON-NLS-1$
    	cache.setResults(id2, result2, "req2" );   //$NON-NLS-1$
    	//use the first one
    	assertNotNull(cache.getResults(id1, new int[]{1, 500}));
    	//add one more result. Because the max size is 1mb, the
    	//third one can not be added
       	cache.setResults(id3, result3, "req3" );  //$NON-NLS-1$
       	assertNull(cache.getResults(id3, new int[]{1, 500})); 
    	//reduced the size and try again. The second one should
    	//be removed
       	result3 = createResults(1000, 1, 1, true);
       	cache.setResults(id3, result3, "req4" );  //$NON-NLS-1$
       	assertNotNull(cache.getResults(id3, new int[]{1, 500})); 
    }
    
    private CacheResults createResults(int size, int firstRow, int rowCnt, boolean isFinal){
    	List[] results = new List[rowCnt];
    	for(int i=0; i<rowCnt; i++){
    		results[i] = new ArrayList();
    	}
    	CacheResults cr = new CacheResults(results, firstRow, isFinal);
    	cr.setSize(size);
    	return cr;
    }
    
    
    @Test public void testComputeSize() throws Exception{
    	int cnt = 1000000;
    	List[] results = new List[cnt];
		List row = new ArrayList();
		row.add("ajwuiotbn0w49yunq9 tjfvwioprkpo23bltplql;galkg"); //$NON-NLS-1$
		row.add(new Long(1242534776));
		row.add(new Double(24235.5476884));
    	for(int i=0; i<cnt; i++){
    		results[i] = row;
    	} 	
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_CONN);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("vdb1", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheResults result = new CacheResults(results, 1, true);
    	cache.setResults(id1, result, "req1" );  //$NON-NLS-1$
    	
        int size = (SizeUtility.IS_64BIT ? 296000000 : 256000000);
    	assertEquals(size, result.getSize());
    }
    
    @Test(expected=MetaMatrixRuntimeException.class) public void testBatchNotContiguous() throws Exception{
    	Properties props = new Properties();
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, "0"); //$NON-NLS-1$
    	props.setProperty(ResultSetCache.RS_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_CONN);
    	ResultSetCache cache = new ResultSetCache(props, new FakeCacheFactory());
    	CacheID id1 = new CacheID("vdb1", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheResults result1 = createResults(1000, 1, 100, false);
    	CacheResults result3 = createResults(2000, 102, 200, true);
    	cache.setResults(id1, result1 , "req1");  //$NON-NLS-1$
   		cache.setResults(id1, result3, "req1" );  //$NON-NLS-1$
    }
    
}
