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
import java.util.Properties;

import org.junit.Test;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings({"nls", "unchecked"})
public class TestObjectTable {

	@Test public void testIterator() throws Exception {
    	String sql = "select x.* from bqt1.smalla, objecttable('ov' passing objectvalue as ov COLUMNS x string 'teiid_row', y integer 'teiid_row_number') x"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList("hello", 1),
        		Arrays.asList("world", 2),
        		Arrays.asList("x", 1),
        		Arrays.asList("y", 2),
        };    

        process(sql, expected);
    }
	
	@Test public void testProjection() throws Exception {
    	String sql = "select y, z from bqt1.smalla, objecttable('ov' passing objectvalue as ov COLUMNS x string 'teiid_row', y integer 'teiid_row_number', z integer 'teiid_row.length') x order by x.x desc limit 1"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList(2, 1),
        };    

        process(sql, expected);
    }
	
	@Test public void testContext() throws Exception {
    	String sql = "select * from objecttable('teiid_context' COLUMNS y string 'teiid_row.userName') as X"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList("user"),
        };    

        process(sql, expected);
    }
	
	public static void process(String sql, List[] expectedResults) throws Exception {    
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	dataManager.addData("SELECT BQT1.SmallA.ObjectValue FROM BQT1.SmallA", new List[] {Collections.singletonList(Arrays.asList("hello", "world")), Collections.singletonList(Arrays.asList("x", null, "y")), Collections.singletonList(null)} );
    	Properties p = new Properties();
		p.put(TransformationMetadata.ALLOWED_LANGUAGES, ObjectTable.DEFAULT_LANGUAGE);
		TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.exampleBQTCached().getMetadataStore(), "bqt", p);
		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata);
        helpProcess(plan, createCommandContext(), dataManager, expectedResults);
    }
	
}
