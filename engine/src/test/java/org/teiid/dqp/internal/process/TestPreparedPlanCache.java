/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.DefaultCacheFactory;
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

@SuppressWarnings("nls")
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
        SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        CacheID id = new CacheID(token, pi, EXAMPLE_QUERY + 1);

        //No PreparedPlan at the begining
        assertNull(cache.get(id));
        //create one
        cache.put(id, Determinism.SESSION_DETERMINISTIC, new PreparedPlan(), null);
        //should have one now
        assertNotNull("Unable to get prepared plan from cache", cache.get(id)); //$NON-NLS-1$
    }

    @Test public void testGet(){
        SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);
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
        SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

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

    // set init size to negative number, which should default to max
    @Test public void testNegativeSizeCacheUsesDefault() {
        CacheConfiguration config = new CacheConfiguration();
        config.setMaxEntries(-1);
        new SessionAwareCache<PreparedPlan>("preparedplan", new DefaultCacheFactory(config), SessionAwareCache.Type.PREPAREDPLAN, 0);
        // -1 means unlimited in the infinispan
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
