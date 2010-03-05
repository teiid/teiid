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

package org.teiid.metadata;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.connector.metadata.runtime.Datatype;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestTransformationMetadata {

	@Test public void testAmbiguousProc() throws Exception {
		Map<String, Datatype> datatypes = new HashMap<String, Datatype>();
        datatypes.put(DataTypeManager.DefaultDataTypes.STRING, new Datatype());
		MetadataFactory mf = new MetadataFactory("x", datatypes, new Properties()); //$NON-NLS-1$
		mf.addProcedure("y"); //$NON-NLS-1$
		MetadataFactory mf1 = new MetadataFactory("x1", datatypes, new Properties()); //$NON-NLS-1$
		mf1.addProcedure("y"); //$NON-NLS-1$
		CompositeMetadataStore cms = new CompositeMetadataStore(Arrays.asList(mf.getMetadataStore(), mf1.getMetadataStore()));
		
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("vdb");
		vdb.setVersion(1);
		
		vdb.addModel(buildModel("x"));
		vdb.addModel(buildModel("x1"));
		
		TransformationMetadata tm = new TransformationMetadata(vdb, cms, null, null);
		DQPWorkContext context = FakeMetadataFactory.buildWorkContext(tm, vdb);

		try {
			tm.getStoredProcedureInfoForProcedure("y"); //$NON-NLS-1$
			fail("expected exception"); //$NON-NLS-1$
		} catch (QueryMetadataException e) {
			assertEquals("Procedure 'y' is ambiguous, use the fully qualified name instead", e.getMessage()); //$NON-NLS-1$
		}
	}
	
	ModelMetaData buildModel(String name) {
		ModelMetaData model = new ModelMetaData();
		model.setName(name);
		model.setModelType(ModelType.getString(ModelType.PHYSICAL));
		model.setVisible(true);
		return model;
	}
	
	@Test public void testAmbiguousTableWithPrivateModel() throws Exception {
		Map<String, Datatype> datatypes = new HashMap<String, Datatype>();
        datatypes.put(DataTypeManager.DefaultDataTypes.STRING, new Datatype());
		MetadataFactory mf = new MetadataFactory("x", datatypes, new Properties()); //$NON-NLS-1$
		mf.addTable("y"); //$NON-NLS-1$
		MetadataFactory mf1 = new MetadataFactory("x1", datatypes, new Properties()); //$NON-NLS-1$
		mf1.addTable("y"); //$NON-NLS-1$
		CompositeMetadataStore cms = new CompositeMetadataStore(Arrays.asList(mf.getMetadataStore(), mf1.getMetadataStore()));
		
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

		TransformationMetadata tm = new TransformationMetadata(vdb, cms, null, null);
		Collection result = tm.getGroupsForPartialName("y"); //$NON-NLS-1$
		assertEquals(2, result.size());

		FakeMetadataFactory.buildWorkContext(tm, vdb);

		model.setVisible(false);

		tm = new TransformationMetadata(vdb, cms, null, null);
		result = tm.getGroupsForPartialName("y"); //$NON-NLS-1$
		assertEquals(1, result.size());
	}
	
}
