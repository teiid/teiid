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

package org.teiid.query.processor;

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings({"unchecked", "nls"})
public class TestArrayTable {
    
	@Test public void testCorrelatedArrayTable() throws Exception {
    	String sql = "select x.* from bqt1.smalla, arraytable(objectvalue COLUMNS x string, y integer) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 1),
        		Arrays.asList("b", 3),
        };    

        process(sql, expected);
    }
	
	@Test public void testCorrelatedArrayTable1() throws Exception {
    	String sql = "select z from bqt1.smalla, arraytable(objectvalue COLUMNS x string, y integer, z long) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(Long.valueOf(2)),
        		Arrays.asList(Long.valueOf(6)),
        };    

        process(sql, expected);
    }
	
	@Test(expected=TeiidProcessingException.class) public void testCorrelatedArrayTable2() throws Exception {
    	String sql = "select y from bqt1.smalla, arraytable(objectvalue COLUMNS y integer) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};    

        process(sql, expected);
    }
	
	@Test public void testCorrelatedArrayTable3() throws Exception {
    	String sql = "select x.* from bqt1.smalla, arraytable(objectvalue COLUMNS x string, y integer, z integer, aa object) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 1, 2, null),
        		Arrays.asList("b", 3, 6, null),
        };    

        process(sql, expected);
    }
	
	public static void process(String sql, List[] expectedResults) throws Exception {    
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	dataManager.addData("SELECT BQT1.SmallA.ObjectValue FROM BQT1.SmallA", new List[] {Collections.singletonList(new Object[] {"a", 1, 2}), Collections.singletonList(new Object[] {"b", 3, 6}), Collections.singletonList(null)} );
    	ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached());
        helpProcess(plan, createCommandContext(), dataManager, expectedResults);
    }
	
}
