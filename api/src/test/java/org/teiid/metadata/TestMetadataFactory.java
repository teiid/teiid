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

package org.teiid.metadata;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;

import org.junit.Test;
import org.teiid.CommandContext;
import org.teiid.UserDefinedAggregate;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;

@SuppressWarnings("nls")
public class TestMetadataFactory {

    @Test public void testSchemaProperties() {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("foo");
        mmd.addProperty("teiid_rel:data-ttl", "1");
        MetadataFactory mf = new MetadataFactory("x", 1, Collections.EMPTY_MAP, mmd);
        Schema s = mf.getSchema();
        assertEquals("foo", s.getName());
        String val = s.getProperty(DataModifiable.DATA_TTL, false);
        assertEquals("1", val);
    }

    @Test public void testCreateFunction() throws NoSuchMethodException, SecurityException {
        FunctionMethod fm = MetadataFactory.createFunctionFromMethod("x", TestMetadataFactory.class.getMethod("someFunction"));
        assertEquals(Boolean.class, fm.getOutputParameter().getJavaType());

        fm = MetadataFactory.createFunctionFromMethod("x", TestMetadataFactory.class.getMethod("someArrayFunction"));
        assertEquals(String[].class, fm.getOutputParameter().getJavaType());
    }

    @Test public void testCreateAggregateFunction() throws NoSuchMethodException, SecurityException {
        FunctionMethod fm = MetadataFactory.createFunctionFromMethod("x", MyUDAF.class.getMethod("addInput", String.class));
        assertEquals(Boolean.class, fm.getOutputParameter().getJavaType());
        assertNotNull(fm.getAggregateAttributes());
    }

    @Test public void testCorrectName() {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("foo");
        HashMap<String, Datatype> types = new HashMap<String, Datatype>();
        Datatype value = new Datatype();
        value.setName("string");
        types.put("string", value);
        MetadataFactory factory = new MetadataFactory("x", 1, types, mmd);
        Table x = factory.addTable("x");
        Column c = factory.addColumn("a.b", "string", x);
        assertEquals("a_b", c.getName());
    }

    @Test public void testDuplicateColumns() {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("foo");
        mmd.addProperty("importer.renameDuplicateColumns", "true");
        HashMap<String, Datatype> types = new HashMap<String, Datatype>();
        Datatype value = new Datatype();
        value.setName("string");
        types.put("string", value);
        MetadataFactory factory = new MetadataFactory("x", 1, types, mmd);
        Table x = factory.addTable("x");
        Column c = factory.addColumn("a_b", "string", x);
        assertEquals("a_b", c.getName());
        c = factory.addColumn("a_B", "string", x);
        assertEquals("a_B_1", c.getName());
    }

    @Test public void testDuplicateTables() {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("foo");
        mmd.addProperty("importer.renameDuplicateTables", "true");
        HashMap<String, Datatype> types = new HashMap<String, Datatype>();
        Datatype value = new Datatype();
        value.setName("string");
        types.put("string", value);
        MetadataFactory factory = new MetadataFactory("x", 1, types, mmd);
        Table x = factory.addTable("x");
        assertEquals("x", x.getName());
        Table x1 = factory.addTable("X");
        assertEquals("X_1", x1.getName());
        Table x2 = factory.addTable("X");
        assertEquals("X_2", x2.getName());
    }

    @Test public void testDuplicateProcedure() {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("foo");
        mmd.addProperty("importer.renameAllDuplicates", "true");
        HashMap<String, Datatype> types = new HashMap<String, Datatype>();
        Datatype value = new Datatype();
        value.setName("string");
        types.put("string", value);
        MetadataFactory factory = new MetadataFactory("x", 1, types, mmd);
        Procedure x = factory.addProcedure("x");
        assertEquals("x", x.getName());
        Procedure x1 = factory.addProcedure("X");
        assertEquals("X_1", x1.getName());
        Procedure x2 = factory.addProcedure("X");
        assertEquals("X_2", x2.getName());
    }

    public static boolean someFunction() {
        return true;
    }

    public static String[] someArrayFunction() {
        return null;
    }

    public static class MyUDAF implements UserDefinedAggregate<Boolean> {
        @Override
        public Boolean getResult(CommandContext commandContext) {
            return null;
        }

        @Override
        public void reset() {

        }

        public void addInput(String val) {

        }
    }

    @Test public void testNameFormat() {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("foo");
        mmd.addProperty("importer.nameFormat", "x_%s_y");
        HashMap<String, Datatype> types = new HashMap<String, Datatype>();
        MetadataFactory factory = new MetadataFactory("x", 1, types, mmd);
        Table x = factory.addTable("x");
        assertEquals("x_x_y", x.getName());

        Procedure p = factory.addProcedure("a%b.c");
        assertEquals("x_a%b.c_y", p.getName());
    }

    @Test public void testPropertyKey() {
        assertEquals("teiid_rest:key", NamespaceContainer.resolvePropertyKey("{http://teiid.org/rest}key"));
    }

}
