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
    
	@Test public void testCorrelatedTextTable() throws Exception {
    	String sql = "select x.* from bqt1.smalla, arraytable(objectvalue COLUMNS x string, y integer) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 1),
        		Arrays.asList("b", 3),
        };    

        process(sql, expected);
    }
	
	@Test public void testCorrelatedTextTable1() throws Exception {
    	String sql = "select z from bqt1.smalla, arraytable(objectvalue COLUMNS x string, y integer, z long) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(Long.valueOf(2)),
        		Arrays.asList(Long.valueOf(6)),
        };    

        process(sql, expected);
    }
	
	@Test(expected=TeiidProcessingException.class) public void testCorrelatedTextTable2() throws Exception {
    	String sql = "select y from bqt1.smalla, arraytable(objectvalue COLUMNS y integer) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};    

        process(sql, expected);
    }
	
	@Test(expected=TeiidProcessingException.class) public void testCorrelatedTextTable3() throws Exception {
    	String sql = "select x.* from bqt1.smalla, arraytable(objectvalue COLUMNS x string, y integer, z integer, aa object) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};    

        process(sql, expected);
    }
	
	public static void process(String sql, List[] expectedResults) throws Exception {    
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	dataManager.addData("SELECT bqt1.smalla.objectvalue FROM bqt1.smalla", new List[] {Collections.singletonList(new Object[] {"a", 1, 2}), Collections.singletonList(new Object[] {"b", 3, 6})} );
    	ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached());
        helpProcess(plan, createCommandContext(), dataManager, expectedResults);
    }
	
}
