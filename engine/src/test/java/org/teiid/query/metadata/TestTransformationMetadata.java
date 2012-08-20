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

package org.teiid.query.metadata;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jboss.vfs.VFS;
import org.junit.Test;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;

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

	private TransformationMetadata exampleTransformationMetadata()
			throws TranslatorException {
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
		table.setSchemaPaths(Arrays.asList("../../x.xsd"));
		table.setResourcePath("/a/b/doc.xmi");
		
		HashMap<String, Resource> resources = new HashMap<String, Resource>();
		resources.put("/x.xsd", new Resource(VFS.getRootVirtualFile(), true));
		
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
		Collection result = tm.getGroupsForPartialName("y"); //$NON-NLS-1$
		assertEquals(2, result.size());

		RealMetadataFactory.buildWorkContext(tm, vdb);

		model.setVisible(false);

		tm = new TransformationMetadata(vdb, cms, null, RealMetadataFactory.SFM.getSystemFunctions(), null);
		result = tm.getGroupsForPartialName("y"); //$NON-NLS-1$
		assertEquals(1, result.size());
	}
	
	@Test public void testElementId() throws Exception {
		TransformationMetadata tm = exampleTransformationMetadata();
		tm.getElementID("x.FoO.coL");
	}
	
	@Test public void testRelativeSchemas() throws Exception {
		TransformationMetadata tm = exampleTransformationMetadata();
		assertEquals(1, tm.getXMLSchemas(tm.getGroupID("x1.doc")).size());
	}
	
	@Test public void testTypeCorrection() throws Exception {
		MetadataFactory mf = new MetadataFactory(null, 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null); //$NON-NLS-1$
		mf.setBuiltinDataTypes(SystemMetadata.getInstance().getSystemStore().getDatatypes());
		
		Table t = mf.addTable("y"); //$NON-NLS-1$
		mf.addColumn("test", "string", t);
		
		MetadataFactory mf1 = UnitTestUtil.helpSerialize(mf);
		
		Column column = mf1.getSchema().getTable("y").getColumns().get(0);
		Datatype dt = column.getDatatype();
		
		assertNotSame(mf.getBuiltinDataTypes().get(dt.getName()), column.getDatatype());
		
		mf1.correctDatatypes(mf.getDataTypes(), mf.getBuiltinDataTypes());
		
		assertSame(mf.getBuiltinDataTypes().get(dt.getName()), column.getDatatype());
	}
	
}
