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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import org.junit.Test;
import org.teiid.core.types.BlobType;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestJSONProcessing {

	@Test public void testJSONParseBlob() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
    	String sql = "select jsonParse(cast(? as blob), false) x"; //$NON-NLS-1$
    	String json = "{\"name\":123}";
    	
        List<?>[] expected = new List[] {
        		Arrays.asList(json),
        };    
    
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(new BlobType(new SerialBlob(json.getBytes(Charset.forName("UTF-16BE"))))));
	}
	
	@Test public void testJSONArray() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
    	String sql = "select jsonArray(1, null, true, {d '2007-01-01'}, jsonParse('{\"name\":123}', true), unescape('\\t\\n?'))"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList("[1,null,true,\"2007-01-01\",{\"name\":123},\"\\t\\n?\"]"),
        };    
    
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testJSONArray_Agg() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
    	String sql = "select jsonArray_agg(e1 order by e1 desc) from pm1.g1"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList("[\"d\",\"a\",\"\\\"b\"]"),
        };    
    
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a"), Arrays.asList("\"b"), Arrays.asList("d")});
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testJSONObject() throws Exception {
    	String sql = "select jsonObject(e1, e2, 1) from pm1.g1 order by e1 limit 1"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList("{\"e1\":null,\"e2\":1,\"expr3\":1}"),
        };    
        FakeDataManager fdm = new FakeDataManager();
        sampleData1(fdm);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, fdm, expected);
	}
	
}
