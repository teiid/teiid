/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
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
        assertEquals("mmuuid:5c2cede9-0e18-4e4c-a531-34507abf0ff8", fm.getUUID());
        assertEquals("sampleFunction", fm.getName());
        assertEquals(1, fm.getInputParameters().size());
        assertEquals("mmuuid:f9ded2ae-9652-414e-b5a9-74185f8703c0", fm.getOutputParameter().getUUID());
        assertNotNull(fm.getInputParameters().get(0).getParent());
    }

}
