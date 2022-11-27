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

package org.teiid.query.metadata;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestTransformationMetadata {

    @Test public void testAmbiguousProc() throws Exception {
        TransformationMetadata tm = exampleTransformationMetadata();

        try {
            tm.getStoredProcedureInfoForProcedure("y"); //$NON-NLS-1$
            fail("expected exception"); //$NON-NLS-1$
        } catch (QueryMetadataException e) {
            assertEquals("TEIID30358 Procedure 'y' is ambiguous, use the fully qualified name instead", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testProcVisibility() throws Exception {
        TransformationMetadata tm = exampleTransformationMetadata();
        VDBMetaData vdb = tm.getVdbMetaData();
        vdb.getModel("x").setVisible(false);
        StoredProcedureInfo spi = tm.getStoredProcedureInfoForProcedure("y"); //$NON-NLS-1$
        assertEquals("x1.y", spi.getProcedureCallableName());
        spi = tm.getStoredProcedureInfoForProcedure("x.y"); //$NON-NLS-1$
        assertEquals("x.y", spi.getProcedureCallableName());
    }

    private TransformationMetadata exampleTransformationMetadata() {
        Map<String, Datatype> datatypes = new HashMap<String, Datatype>();
        Datatype dt = new Datatype();
        dt.setName(DataTypeManager.DefaultDataTypes.STRING);
        dt.setJavaClassName(String.class.getCanonicalName());
        datatypes.put(DataTypeManager.DefaultDataTypes.STRING, dt);
        MetadataFactory mf = new MetadataFactory(null, 1, "x", datatypes, new Properties(), null); //$NON-NLS-1$
        mf.addProcedure("y"); //$NON-NLS-1$

        Table t = mf.addTable("foo");
        mf.addColumn("col", DataTypeManager.DefaultDataTypes.STRING, t);

        MetadataFactory mf1 = new MetadataFactory(null, 1, "x1", datatypes, new Properties(), null); //$NON-NLS-1$
        mf1.addProcedure("y"); //$NON-NLS-1$

        Table table = mf1.addTable("doc");
        table.setResourcePath("/a/b/doc.xmi");

        HashMap<String, VDBResources.Resource> resources = new HashMap<String, VDBResources.Resource>();
        resources.put("/x.xsd", new VDBResources.Resource(Mockito.mock(VirtualFile.class)));

        CompositeMetadataStore cms = new CompositeMetadataStore(Arrays.asList(mf.asMetadataStore(), mf1.asMetadataStore()));

        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("vdb");
        vdb.setVersion(1);

        vdb.addModel(buildModel("x"));
        vdb.addModel(buildModel("x1"));
        vdb.addModel(buildModel("y"));

        return new TransformationMetadata(vdb, cms, resources, RealMetadataFactory.SFM.getSystemFunctions(), null);
    }

    ModelMetaData buildModel(String name) {
        ModelMetaData model = new ModelMetaData();
        model.setName(name);
        model.setModelType(Model.Type.PHYSICAL);
        model.setVisible(true);
        return model;
    }

    @Test public void testAmbiguousTableWithPrivateModel() throws Exception {
        Map<String, Datatype> datatypes = new HashMap<String, Datatype>();
        Datatype dt = new Datatype();
        dt.setName(DataTypeManager.DefaultDataTypes.STRING);
        dt.setJavaClassName(String.class.getCanonicalName());
        datatypes.put(DataTypeManager.DefaultDataTypes.STRING, dt);
        MetadataFactory mf = new MetadataFactory(null, 1, "x", datatypes, new Properties(), null); //$NON-NLS-1$
        mf.addTable("y"); //$NON-NLS-1$
        MetadataFactory mf1 = new MetadataFactory(null, 1, "x1", datatypes, new Properties(), null); //$NON-NLS-1$
        mf1.addTable("y"); //$NON-NLS-1$
        CompositeMetadataStore cms = new CompositeMetadataStore(Arrays.asList(mf.asMetadataStore(), mf1.asMetadataStore()));

        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("foo");
        vdb.setVersion(1);

        ModelMetaData model = new ModelMetaData();
        model.setName("x1");
        vdb.addModel(model);

        ModelMetaData model2 = new ModelMetaData();
        model2.setName("x");
        model2.setVisible(true);
        vdb.addModel(model2);

        TransformationMetadata tm = new TransformationMetadata(vdb, cms, null, RealMetadataFactory.SFM.getSystemFunctions(), null);
        Collection<String> result = tm.getGroupsForPartialName("y"); //$NON-NLS-1$
        assertEquals(2, result.size());

        RealMetadataFactory.buildWorkContext(tm, vdb);

        model.setVisible(false);

        tm = new TransformationMetadata(vdb, cms, null, RealMetadataFactory.SFM.getSystemFunctions(), null);
        result = tm.getGroupsForPartialName("y"); //$NON-NLS-1$
        assertEquals(1, result.size());

        TransformationMetadata designTime = tm.getDesignTimeMetadata();
        result = designTime.getGroupsForPartialName("y"); //$NON-NLS-1$
        assertEquals(2, result.size());

        vdb.addProperty("hidden-qualified", "true");
        designTime = tm.getDesignTimeMetadata();
        result = designTime.getGroupsForPartialName("y"); //$NON-NLS-1$
        assertEquals(1, result.size());
    }

    @Test public void testElementId() throws Exception {
        TransformationMetadata tm = exampleTransformationMetadata();
        tm.getElementID("x.FoO.coL");
    }

    @Test public void testTypeCorrection() throws Exception {
        MetadataFactory mf = new MetadataFactory(null, 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null); //$NON-NLS-1$

        Table t = mf.addTable("y"); //$NON-NLS-1$
        mf.addColumn("test", "string", t);
        mf.addColumn("array", "string[]", t);
        Datatype unknown = new Datatype();
        unknown.setName("unknown");
        Column col = mf.addColumn("arg", "string", t);
        col.setDatatype(unknown, false, 0);
        MetadataFactory mf1 = UnitTestUtil.helpSerialize(mf);

        Column column = mf1.getSchema().getTable("y").getColumns().get(0);
        Datatype dt = column.getDatatype();

        assertNotSame(mf.getDataTypes().get(dt.getName()), column.getDatatype());

        assertEquals(1, mf1.getSchema().getTable("y").getColumns().get(1).getArrayDimensions());

        mf1.correctDatatypes(mf.getDataTypes());

        assertSame(mf.getDataTypes().get(dt.getName()), column.getDatatype());

        assertEquals(1, mf1.getSchema().getTable("y").getColumns().get(1).getArrayDimensions());
    }

    @Test public void testAdminHidden() throws Exception {
        TransformationMetadata tm = exampleTransformationMetadata();
        tm.setHiddenResolvable(false);
        VDBMetaData vdb = tm.getVdbMetaData();
        vdb.getModel("x").setVisible(false);
        try {
            StoredProcedureInfo spi = tm.getStoredProcedureInfoForProcedure("x.y"); //$NON-NLS-1$
            fail(); //not visible
        } catch (QueryMetadataException e) {
        }

        DQPWorkContext.getWorkContext().setAdmin(true);
        try {
            StoredProcedureInfo spi = tm.getStoredProcedureInfoForProcedure("x.y"); //$NON-NLS-1$
            assertEquals("x.y", spi.getProcedureCallableName());
        } finally {
            DQPWorkContext.setWorkContext(new DQPWorkContext());
        }
    }

}
