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

package org.teiid.metadata.index;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Map;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;

@SuppressWarnings("nls")
public class TestMultipleModelIndexes {

	@Test public void testMultiple() throws Exception {
		TransformationMetadata tm = VDBMetadataFactory.getVDBMetadata(UnitTestUtil.getTestDataPath() + "/ZZZ.vdb");
		Collection<String> names = tm.getGroupsForPartialName("PRODUCTDATA");
		assertEquals(1, names.size());
		names = tm.getGroupsForPartialName("PARTS");
		assertEquals(1, names.size());
		
		//ensure that datatypes are set
		Table t = tm.getGroupID(names.iterator().next());
		assertNotNull(t.getColumns().get(0).getDatatype());
	}
	
	@Test public void testUniqueReferencedKey() throws Exception {
		TransformationMetadata tm = VDBMetadataFactory.getVDBMetadata(UnitTestUtil.getTestDataPath() + "/keys.vdb");
		Collection fks = tm.getForeignKeysInGroup(tm.getGroupID("x.a"));
		assertEquals(1, fks.size());
		Object pk = tm.getPrimaryKeyIDForForeignKeyID(fks.iterator().next());
		assertNotNull(pk);
	}
	
	@Test public void testIndex() throws Exception {
		TransformationMetadata tm = VDBMetadataFactory.getVDBMetadata(UnitTestUtil.getTestDataPath() + "/ora.vdb");
		Collection indexes = tm.getIndexesInGroup(tm.getGroupID("ORACLE_BQT.SMALLA"));
		assertEquals(1, indexes.size());
	}
	
	@Test public void testSchemaLoad() throws Exception {
		TransformationMetadata tm = VDBMetadataFactory.getVDBMetadata(UnitTestUtil.getTestDataPath() + "/Test.vdb");
		
		//ensure that datatypes are set
		Table t = tm.getGroupID("Northwind.Northwind.dbo.Employees");
		assertFalse(t.isVirtual());
	}
	
	@Test public void test81Schema() throws Exception {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(UnitTestUtil.getTestDataFile("schema.ser")));
		Schema schema = (Schema) ois.readObject();
		assertNotNull(schema.getFunctions());
	}
	
	@Test public void testFunctionMetadata() throws Exception {
		TransformationMetadata tm = VDBMetadataFactory.getVDBMetadata(UnitTestUtil.getTestDataPath() + "/TEIIDDES992_VDB.vdb");
		Map<String, FunctionMethod> functions = tm.getMetadataStore().getSchema("TEIIDDES992").getFunctions();
		assertEquals(1, functions.size());
		FunctionMethod fm = functions.values().iterator().next();
		assertEquals("sampleFunction", fm.getName());
		assertEquals(1, fm.getInputParameters().size());
	}
	
}
