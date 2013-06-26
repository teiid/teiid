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

package org.teiid.deployers;

import static org.junit.Assert.*;

import java.util.concurrent.Executor;

import org.junit.Test;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionFactory;

@SuppressWarnings("nls")
public class TestVDBStatusChecker {

	@Test public void testDataSourceReplaced() throws Exception {
		final VDBRepository repo = new VDBRepository();
		repo.setSystemFunctionManager(RealMetadataFactory.SFM);
		repo.start();
		
		VDBStatusChecker vsc = new VDBStatusChecker() {
			
			@Override
			public VDBRepository getVDBRepository() {
				return repo;
			}
			
			@Override
			public Executor getExecutor() {
				return null;
			}
		};
		VDBTranslatorMetaData factory = new VDBTranslatorMetaData();
		factory.setExecutionFactoryClass(ExecutionFactory.class);
		
		assertFalse(vsc.dataSourceReplaced("x", 1, "y", "z", "t", "dsName"));
		
		MetadataStore metadataStore = RealMetadataFactory.exampleBQTCached().getMetadataStore();
		VDBMetaData vdb = TestCompositeVDB.createVDBMetadata(metadataStore, "bqt");
		
		ConnectorManagerRepository cmr = new ConnectorManagerRepository();
		cmr.setProvider(new ExecutionFactoryProvider() {
			
			@Override
			public ExecutionFactory<Object, Object> getExecutionFactory(String name)
					throws ConnectorManagerException {
				return new ExecutionFactory<Object, Object>();
			}
		});
		ConnectorManager mgr = new ConnectorManager("oracle", "dsName");
		ExecutionFactory ef1 = new ExecutionFactory();
		mgr.setExecutionFactory(ef1);
		cmr.addConnectorManager("BQT1", mgr);
		repo.addVDB(vdb, metadataStore, null, null, cmr);
		
		assertTrue(vsc.dataSourceReplaced("bqt", 1, "BQT1", "BQT1", "oracle", "dsName1"));
		ExecutionFactory ef = cmr.getConnectorManager("BQT1").getExecutionFactory();
		assertSame(ef, ef1);
		assertFalse(vsc.dataSourceReplaced("bqt", 1, "BQT1", "BQT1", "sqlserver", "dsName1"));
		ExecutionFactory ef2 = cmr.getConnectorManager("BQT1").getExecutionFactory();
		assertNotNull(ef2);
		assertNotSame(ef, ef2);
		assertTrue(vsc.dataSourceReplaced("bqt", 1, "BQT1", "BQT1", "oracle", "dsName2"));
		ef = cmr.getConnectorManager("BQT1").getExecutionFactory();
		assertNotNull(ef);
		assertNotSame(ef, ef2);
	}
	
}
