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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.dqp.internal.process.PreparedPlanCache;
import org.teiid.dqp.internal.process.PreparedPlanCache.CacheID;

import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.parser.ParseInfo;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.relational.ProjectNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.sql.lang.Command;

public class TestPreparedPlanCache {
    private static final String EXAMPLE_QUERY = "SELECT * FROM table"; //$NON-NLS-1$
	private final static DQPWorkContext token = new DQPWorkContext();
	private final static  DQPWorkContext token2 = new DQPWorkContext();
	
	private final static ParseInfo pi = new ParseInfo();
	
	@BeforeClass public static void setUpOnce() {
		token.setVdbName("foo"); //$NON-NLS-1$
		token.setVdbVersion("1"); //$NON-NLS-1$
		token2.setVdbName("foo"); //$NON-NLS-1$
		token2.setVdbVersion("2"); //$NON-NLS-1$
	}
    
    //====Tests====//
    @Test public void testPutPreparedPlan(){
    	PreparedPlanCache cache = new PreparedPlanCache();
    	
    	CacheID id = new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 1);
    	
    	//No PreparedPlan at the begining
    	assertNull(cache.getPreparedPlan(id));
    	//create one
    	cache.putPreparedPlan(id, true, new PreparedPlanCache.PreparedPlan());
    	//should have one now
    	assertNotNull("Unable to get prepared plan from cache", cache.getPreparedPlan(id)); //$NON-NLS-1$
    }
    
    @Test public void testGetPreparedPlan(){
    	PreparedPlanCache cache = new PreparedPlanCache();
    	helpPutPreparedPlans(cache, token, 0, 10);
    	helpPutPreparedPlans(cache, token2, 0, 15);
    	
    	//read an entry for session2 (token2)
    	PreparedPlanCache.PreparedPlan pPlan = cache.getPreparedPlan(new PreparedPlanCache.CacheID(token2, pi, EXAMPLE_QUERY + 12));
    	assertNotNull("Unable to get prepared plan from cache", pPlan); //$NON-NLS-1$
    	assertEquals("Error getting plan from cache", new RelationalPlan(new ProjectNode(12)).toString(), pPlan.getPlan().toString()); //$NON-NLS-1$
    	assertEquals("Error getting command from cache", EXAMPLE_QUERY + 12, pPlan.getCommand().toString()); //$NON-NLS-1$
    	assertNotNull("Error getting plan description from cache", pPlan.getAnalysisRecord().getQueryPlan()); //$NON-NLS-1$
    	assertEquals("Error gettting reference from cache", "ref12", pPlan.getReferences().get(0)); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testClearAll(){
    	PreparedPlanCache cache = new PreparedPlanCache();
    	
    	//create one for each session token
    	helpPutPreparedPlans(cache, token, 1, 1);
    	helpPutPreparedPlans(cache, token2, 1, 1);
    	//should have one
    	assertNotNull("Unable to get prepared plan from cache for token", cache.getPreparedPlan(new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 1))); //$NON-NLS-1$
    	cache.clearAll();
    	//should not exist for token
    	assertNull("Failed remove from cache", cache.getPreparedPlan(new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 1))); //$NON-NLS-1$ 
    	//should not exist for token2
    	assertNull("Unable to get prepared plan from cache for token2", cache.getPreparedPlan(new PreparedPlanCache.CacheID(token2, pi, EXAMPLE_QUERY + 1))); //$NON-NLS-1$ 
    }
    
    @Test public void testMaxSize(){
        PreparedPlanCache cache = new PreparedPlanCache(100);
        helpPutPreparedPlans(cache, token, 0, 101);
        //the first one should be gone because the max size is 100
        assertNull(cache.getPreparedPlan(new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 0))); 
        
        assertNotNull(cache.getPreparedPlan(new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 12))); 
        helpPutPreparedPlans(cache, token, 102, 50);
        //"sql12" should still be there based on lru  policy
        assertNotNull(cache.getPreparedPlan(new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 12))); 
        
        helpPutPreparedPlans(cache, token2, 0, 121);
        helpPutPreparedPlans(cache, token, 0, 50);
        assertTrue(cache.getSpaceUsed() <= 100);
    }
    
    @Test public void testZeroSizeCache() {
        // Create with 0 size cache
        PreparedPlanCache cache = new PreparedPlanCache(0);
        assertEquals(0, cache.getSpaceAllowed());
        
        // Add 1 plan and verify it is not in the cache
        helpPutPreparedPlans(cache, token, 0, 1);
        assertNull(cache.getPreparedPlan(new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 0))); 
        assertEquals(0, cache.getSpaceUsed());
        
        // Add another plan and verify it is not in the cache
        helpPutPreparedPlans(cache, token, 1, 1);
        assertNull(cache.getPreparedPlan(new PreparedPlanCache.CacheID(token, pi, EXAMPLE_QUERY + 1))); 
        assertEquals(0, cache.getSpaceUsed());        
    }
    
    // set init size to negative number, which should default to 100 (default)
    @Test public void testNegativeSizeCacheUsesDefault() {
        PreparedPlanCache negativeSizedCache = new PreparedPlanCache(-1000);
        PreparedPlanCache defaultSizedCache = new PreparedPlanCache();
        
        assertEquals(defaultSizedCache.getSpaceAllowed(), negativeSizedCache.getSpaceAllowed());
        assertEquals(PreparedPlanCache.DEFAULT_MAX_SIZE_TOTAL, negativeSizedCache.getSpaceAllowed());                       
    }
    
    //====Help methods====//
    private void helpPutPreparedPlans(PreparedPlanCache cache, DQPWorkContext session, int start, int count){
    	for(int i=0; i<count; i++){
    		Command dummy;
			try {
				dummy = QueryParser.getQueryParser().parseCommand(EXAMPLE_QUERY + (start + i)); 
			} catch (QueryParserException e) {
				throw new RuntimeException(e);
			}
	    	CacheID id = new PreparedPlanCache.CacheID(session, pi, dummy.toString());

	    	PreparedPlanCache.PreparedPlan pPlan = new PreparedPlanCache.PreparedPlan();
    		cache.putPreparedPlan(id, true, pPlan);
    		pPlan.setCommand(dummy); 
    		pPlan.setPlan(new RelationalPlan(new ProjectNode(i)));
            Map props = new HashMap();
            props.put("desc", "desc"+i); //$NON-NLS-1$ //$NON-NLS-2$
            AnalysisRecord analysisRecord = new AnalysisRecord(true, true, false);
            analysisRecord.setQueryPlan(props);
    		pPlan.setAnalysisRecord(analysisRecord);
    		ArrayList refs = new ArrayList();
    		refs.add("ref"+i); //$NON-NLS-1$
    		pPlan.setReferences(refs);
    	}
    }
    
}
