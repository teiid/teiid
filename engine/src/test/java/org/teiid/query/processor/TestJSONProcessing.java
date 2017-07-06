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
	
	@Test public void testJSONObjectWithNestedJson() throws Exception {
    	String sql = "select jsonObject(jsonObject(e1), e2, jsonarray(e3, 2), 1) from pm1.g1 order by e1 limit 1"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList("{\"expr1\":{\"e1\":null},\"e2\":1,\"expr3\":[false,2],\"expr4\":1}"),
        };    
        FakeDataManager fdm = new FakeDataManager();
        sampleData1(fdm);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, fdm, expected);
	}
	
	@Test public void testJSONObjectWithNestedJson1() throws Exception {
    	String sql = "select jsonObject(jsonObject(e2, 1, jsonObject((select jsonArray_agg(e2) from pm1.g2 where e1 = pm1.g1.e1), e1))) from pm1.g1 order by e1 nulls last limit 1"; //$NON-NLS-1$
    	
        List<?>[] expected = new List[] {
        		Arrays.asList("{\"expr1\":{\"e2\":0,\"expr2\":1,\"expr3\":{\"expr1\":[0,3,0],\"e1\":\"a\"}}}"),
        };    
        FakeDataManager fdm = new FakeDataManager();
        fdm.setBlockOnce();
        sampleData1(fdm);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
      	helpProcess(plan, fdm, expected);
	}
	
}
