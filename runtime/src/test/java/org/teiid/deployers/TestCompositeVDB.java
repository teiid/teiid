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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.vdb.runtime.VDBKey;

@SuppressWarnings("nls")
public class TestCompositeVDB {
	
	public static TransformationMetadata createTransformationMetadata(MetadataStore metadataStore, String vdbName) throws Exception {
    	CompositeVDB cvdb = createCompositeVDB(metadataStore, vdbName);
    	VDBMetaData vdb = cvdb.getVDB();
    	return vdb.getAttachment(TransformationMetadata.class);
	}

	private static CompositeVDB createCompositeVDB(MetadataStore metadataStore,
			String vdbName) {
		VDBMetaData vdbMetaData = new VDBMetaData();
    	vdbMetaData.setName(vdbName); //$NON-NLS-1$
    	vdbMetaData.setVersion(1);
    	for (Schema schema : metadataStore.getSchemas().values()) {
			vdbMetaData.addModel(RealMetadataFactory.createModel(schema.getName(), schema.isPhysical()));
		}
    	MetadataStoreGroup metaGroup = new MetadataStoreGroup();
    	metaGroup.addStore(metadataStore);
    	
    	ConnectorManagerRepository cmr = new ConnectorManagerRepository();
    	cmr.addConnectorManager("source", getConnectorManager("bqt1", "FakeTranslator", "FakeConnection", getFuncsOne()));
    	cmr.addConnectorManager("source2", getConnectorManager("bqt2", "FakeTranslator2", "FakeConnection2", getFuncsTwo()));
    	
    	CompositeVDB cvdb = new CompositeVDB(vdbMetaData, metaGroup, null, null, RealMetadataFactory.SFM.getSystemFunctions(),cmr);
		return cvdb;
	}
	
	private static ConnectorManager getConnectorManager(String modelName, String translatorName, String connectionName,  List<FunctionMethod> funcs) {
		final ExecutionFactory<Object, Object> ef = Mockito.mock(ExecutionFactory.class);
		
		Mockito.stub(ef.getPushDownFunctions()).toReturn(funcs);
		
		ConnectorManager cm = new ConnectorManager(translatorName,connectionName) { 
			protected ExecutionFactory getExecutionFactory() {
				return ef;
			}
		};
		cm.setModelName(modelName); 
		cm.start();
		return cm;
	}
	
	private static List<FunctionMethod> getFuncsOne() {
    	List<FunctionMethod> funcs = new ArrayList<FunctionMethod>();
        funcs.add(new FunctionMethod("echo", "echo", "misc", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
                new FunctionParameter[] {new FunctionParameter("columnName", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
        funcs.add(new FunctionMethod("duplicate_func", "duplicate", "misc", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
	                new FunctionParameter[] {new FunctionParameter("columnName", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
    	return funcs;		
	}
	
	private static List<FunctionMethod> getFuncsTwo() {
    	List<FunctionMethod> funcs = new ArrayList<FunctionMethod>();
        funcs.add(new FunctionMethod("duplicate_func", "duplicate", "misc", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
                new FunctionParameter[] {new FunctionParameter("columnName", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
    	funcs.add(new FunctionMethod("duplicate_func", "duplicate", "misc", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
	                new FunctionParameter[] {new FunctionParameter("c1", DataTypeManager.DefaultDataTypes.INTEGER, ""),
        		    new FunctionParameter("c2", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
    	return funcs;		
	}
	
	
	private void helpResolve(String sql) throws Exception {
		TransformationMetadata metadata = createTransformationMetadata(RealMetadataFactory.exampleBQTCached().getMetadataStore(), "bqt");
		Command command = QueryParser.getQueryParser().parseCommand(sql);
		QueryResolver.resolveCommand(command, metadata);		
	}
	
	@Test
	public void testSourceMetadataStoreFunction() throws Exception {
		helpResolve("SELECT bqt1.reverse(BQT1.SmallA.INTKEY) FROM BQT1.SmallA");
	}
	
	@Test
	public void testTranslatorDefinedFunction() throws Exception {
		helpResolve("SELECT SYS.echo(BQT1.SmallA.INTKEY) FROM BQT1.SmallA");
	}
	
	@Test
	public void testPartialUDFName() throws Exception {
		helpResolve("SELECT echo(BQT1.SmallA.STRINGKEY) FROM BQT1.SmallA");
	}
	
	@Test
	public void testFullyQualifiedDuplicate() throws Exception {
		helpResolve("SELECT SYS.duplicate_func(BQT1.SmallA.STRINGKEY) FROM BQT1.SmallA");
	}	
	
	@Test
	public void testNonQualifiedDuplicateWithDifferentSignature() throws Exception {
		helpResolve("SELECT duplicate_func(BQT1.SmallA.INTKEY, BQT1.SmallA.STRINGKEY) FROM BQT1.SmallA");
	}
	
	@Test
	public void testNonQualifiedDuplicate() throws Exception {
		helpResolve("SELECT duplicate_func(BQT1.SmallA.INTKEY) FROM BQT1.SmallA");
	}		
	
	@Test public void testRemoveChild() throws Exception {
		CompositeVDB vdb = createCompositeVDB(RealMetadataFactory.exampleBQTStore(), "bqt");
		vdb.removeChild(new VDBKey("foo", 1));
		assertNotNull(vdb.getVDB());
	}
	
}
