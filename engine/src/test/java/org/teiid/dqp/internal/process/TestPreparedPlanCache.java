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

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.util.CommandContext;


public class TestPreparedPlanCache {
    private static final String EXAMPLE_QUERY = "SELECT * FROM table"; //$NON-NLS-1$
	private final static DQPWorkContext token = new DQPWorkContext();
	private final static  DQPWorkContext token2 = new DQPWorkContext();
	
	private final static ParseInfo pi = new ParseInfo();
	
	@BeforeClass public static void setUpOnce() {
		token.getSession().setVDBName("foo"); //$NON-NLS-1$
		token.getSession().setVDBVersion(1); 
		token2.getSession().setVDBName("foo"); //$NON-NLS-1$
		token2.getSession().setVDBVersion(2); 
	}
    
    //====Tests====//
    @Test public void testPutPreparedPlan(){
    	SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>();
    	
    	CacheID id = new CacheID(token, pi, EXAMPLE_QUERY + 1);
    	
    	//No PreparedPlan at the begining
    	assertNull(cache.get(id));
    	//create one
    	cache.put(id, Determinism.SESSION_DETERMINISTIC, new PreparedPlan(), null);
    	//should have one now
    	assertNotNull("Unable to get prepared plan from cache", cache.get(id)); //$NON-NLS-1$
    }
    
    @Test public void testGet(){
    	SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>();
    	helpPutPreparedPlans(cache, token, 0, 10);
    	helpPutPreparedPlans(cache, token2, 0, 15);
    	
    	//read an entry for session2 (token2)
    	PreparedPlan pPlan = cache.get(new CacheID(token2, pi, EXAMPLE_QUERY + 12));
    	assertNotNull("Unable to get prepared plan from cache", pPlan); //$NON-NLS-1$
    	assertEquals("Error getting plan from cache", new RelationalPlan(new ProjectNode(12)).toString(), pPlan.getPlan().toString()); //$NON-NLS-1$
    	assertEquals("Error getting command from cache", EXAMPLE_QUERY + 12, pPlan.getCommand().toString()); //$NON-NLS-1$
    	assertNotNull("Error getting plan description from cache", pPlan.getAnalysisRecord()); //$NON-NLS-1$
    	assertEquals("Error gettting reference from cache", new Reference(1), pPlan.getReferences().get(0)); //$NON-NLS-1$
    }
    
    @Test public void testClearAll(){
    	SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>();
    	
    	//create one for each session token
    	helpPutPreparedPlans(cache, token, 1, 1);
    	helpPutPreparedPlans(cache, token2, 1, 1);
    	//should have one
    	assertNotNull("Unable to get prepared plan from cache for token", cache.get(new CacheID(token, pi, EXAMPLE_QUERY + 1))); //$NON-NLS-1$
    	cache.clearAll();
    	//should not exist for token
    	assertNull("Failed remove from cache", cache.get(new CacheID(token, pi, EXAMPLE_QUERY + 1))); //$NON-NLS-1$ 
    	//should not exist for token2
    	assertNull("Unable to get prepared plan from cache for token2", cache.get(new CacheID(token2, pi, EXAMPLE_QUERY + 1))); //$NON-NLS-1$ 
    }
    
    @Test public void testMaxSize(){
        SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>(100);
        helpPutPreparedPlans(cache, token, 0, 101);
        //the first one should be gone because the max size is 100
        assertNull(cache.get(new CacheID(token, pi, EXAMPLE_QUERY + 0))); 
        
        assertNotNull(cache.get(new CacheID(token, pi, EXAMPLE_QUERY + 12))); 
        helpPutPreparedPlans(cache, token, 102, 50);
        //"sql12" should still be there based on lru  policy
        assertNotNull(cache.get(new CacheID(token, pi, EXAMPLE_QUERY + 12))); 
        
        helpPutPreparedPlans(cache, token2, 0, 121);
        helpPutPreparedPlans(cache, token, 0, 50);
        assertTrue(cache.getTotalCacheEntries() <= 100);
    }
    
    @Test public void testZeroSizeCache() {
        // Create with 0 size cache
        SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>(0);
        assertEquals(0, cache.getSpaceAllowed());
        
        // Add 1 plan and verify it is not in the cache
        helpPutPreparedPlans(cache, token, 0, 1);
        assertNull(cache.get(new CacheID(token, pi, EXAMPLE_QUERY + 0))); 
        assertEquals(0, cache.getTotalCacheEntries());
        
        // Add another plan and verify it is not in the cache
        helpPutPreparedPlans(cache, token, 1, 1);
        assertNull(cache.get(new CacheID(token, pi, EXAMPLE_QUERY + 1))); 
        assertEquals(0, cache.getTotalCacheEntries());        
    }
    
    // set init size to negative number, which should default to max
    @Test public void testNegativeSizeCacheUsesDefault() {
        SessionAwareCache<PreparedPlan> negativeSizedCache = new SessionAwareCache<PreparedPlan>(-1);
        
        assertEquals(Integer.MAX_VALUE, negativeSizedCache.getSpaceAllowed());                       
    }
    
    //====Help methods====//
    private void helpPutPreparedPlans(SessionAwareCache<PreparedPlan> cache, DQPWorkContext session, int start, int count){
    	for(int i=0; i<count; i++){
    		Command dummy;
			try {
				dummy = QueryParser.getQueryParser().parseCommand(EXAMPLE_QUERY + (start + i)); 
			} catch (QueryParserException e) {
				throw new RuntimeException(e);
			}
	    	CacheID id = new CacheID(session, pi, dummy.toString());

	    	PreparedPlan pPlan = new PreparedPlan();
    		cache.put(id, Determinism.SESSION_DETERMINISTIC, pPlan, null);
    		pPlan.setCommand(dummy); 
    		pPlan.setPlan(new RelationalPlan(new ProjectNode(i)), new CommandContext());
            AnalysisRecord analysisRecord = new AnalysisRecord(true, false);
    		pPlan.setAnalysisRecord(analysisRecord);
    		ArrayList<Reference> refs = new ArrayList<Reference>();
    		refs.add(new Reference(1));
    		pPlan.setReferences(refs);
    	}
    }
    
}
