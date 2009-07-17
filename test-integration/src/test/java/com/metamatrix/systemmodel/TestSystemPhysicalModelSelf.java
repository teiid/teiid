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

package com.metamatrix.systemmodel;

import java.util.ArrayList;
import java.util.List;

import org.teiid.metadata.index.VDBMetadataFactory;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.cdk.IConnectorHost;
import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.connector.metadata.IndexConnector;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.metadata.runtime.FakeMetadataService;

public class TestSystemPhysicalModelSelf extends TestCase {

	static FakeMetadataService metadataService = null;
	static IConnectorHost host = null;

	static IConnectorHost loadConnectorHost() throws Exception {
    	TranslationUtility utility = new TranslationUtility(VDBMetadataFactory.getVDBMetadata(Thread.currentThread().getContextClassLoader().getResource("System.vdb"))); //$NON-NLS-1$
        ConnectorHost host = new ConnectorHost(new IndexConnector(), null, utility);

        metadataService = new FakeMetadataService(Thread.currentThread().getContextClassLoader().getResource("System.vdb")); //$NON-NLS-1$
		host.addResourceToConnectorEnvironment(DQPServiceNames.METADATA_SERVICE, metadataService);
		host.setSecurityContext("System", "1", "testUser", null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		return host;
	}

    static void oneTimeTearDown() throws Exception {
    	if (metadataService != null) {
    		metadataService.clear();
    	}
    }
    
    static void oneTimeSetUp() throws Exception {
    	host = loadConnectorHost();
    }
    
    public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestSystemPhysicalModelSelf.class);
		return createOnceRunSuite(suite);
	}    
    
    public static TestSetup createOnceRunSuite(TestSuite suite) {
		TestSetup wrapper = new TestSetup(suite) {
			@Override
			protected void setUp() throws Exception {
				oneTimeSetUp();
			}
			@Override
			protected void tearDown() throws Exception {
				oneTimeTearDown();
			}
		};
		return wrapper;
	} 	
	
	private void execute(String query) throws Exception {
		execute(query, null);
	}

	private void execute(String query, String[] expected) throws Exception {
		List<ArrayList<String>> results = host.executeCommand(query);
		if (expected != null) {
			//assertEquals("number results does not match", results.size(),expected.length);
			int i = 0;
			for (ArrayList<String> result : results) {
				String resultStr = concat(result); 
				assertEquals(expected[i++], resultStr);
			}
		}
	}
	
	private String concat(ArrayList<String> list) {
		StringBuilder sb = new StringBuilder();
		for(String msg:list) {
			sb.append(msg);
		}
		return sb.toString();
	}

	public void testSystemPhysical_ACCESS_PATTERNS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_MODEL_NAME()
			throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_NAME_IN_SOURCE()
			throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_OBJECT_TYPE()
			throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_TABLE_NAME()
			throws Exception {
		execute("select  TABLE_NAME from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_UUID_OF_TABLE()
			throws Exception {
		execute("select  UUID_OF_TABLE from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ACCESS_PATTERNS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.ACCESS_PATTERNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_KEY_TYPE() throws Exception {
		execute("select  KEY_TYPE from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_NAME_IN_SOURCE() throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_TABLE_NAME() throws Exception {
		execute("select  TABLE_NAME from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_UUID_OF_TABLE() throws Exception {
		execute("select  UUID_OF_TABLE from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ALL_KEYS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.ALL_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ANNOTATIONS_ANNOTATED_UUID()
			throws Exception {
		execute("select  ANNOTATED_UUID from SystemPhysical.ANNOTATIONS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_ANNOTATIONS_DESCRIPTION() throws Exception {
		execute("select  DESCRIPTION from SystemPhysical.ANNOTATIONS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_CHAR_OCTET_LENGTH() throws Exception {
		execute("select  CHAR_OCTET_LENGTH from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_DATATYPE_UUID() throws Exception {
		execute("select  DATATYPE_UUID from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_DEFAULT_VALUE() throws Exception {
		execute("select  DEFAULT_VALUE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_FORMAT() throws Exception {
		execute("select  FORMAT from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_IS_AUTO_INCREMENTED()
			throws Exception {
		execute("select  IS_AUTO_INCREMENTED from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_IS_CASE_SENSITIVE() throws Exception {
		execute("select  IS_CASE_SENSITIVE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_IS_CURRENCY() throws Exception {
		execute("select  IS_CURRENCY from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_IS_LENGTH_FIXED() throws Exception {
		execute("select  IS_LENGTH_FIXED from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void test_SystemPhysical_COLUMNS_IS_SELECTABLE() throws Exception {
		execute("select IS_SELECTABLE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_IS_SIGNED() throws Exception {
		execute("select  IS_SIGNED from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_LENGTH() throws Exception {
		execute("select  LENGTH from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_MAX_VALUE() throws Exception {
		execute("select  MAX_VALUE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_MIN_VALUE() throws Exception {
		execute("select  MIN_VALUE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_NAME_IN_SOURCE() throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_NULL_TYPE() throws Exception {
		execute("select  NULL_TYPE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_PARENT_NAME() throws Exception {
		execute("select  PARENT_NAME from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_PARENT_PATH() throws Exception {
		execute("select  PARENT_PATH from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_PARENT_UUID() throws Exception {
		execute("select  PARENT_UUID from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_POSITION() throws Exception {
		execute("select  POSITION from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_PRECISION() throws Exception {
		execute("select  PRECISION from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_RADIX() throws Exception {
		execute("select  RADIX from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_RUNTIME_TYPE_NAME() throws Exception {
		execute("select  RUNTIME_TYPE_NAME from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_SCALE() throws Exception {
		execute("select  SCALE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_SEARCH_TYPE() throws Exception {
		execute("select  SEARCH_TYPE from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_SUPPORTS_UPDATES() throws Exception {
		execute("select  SUPPORTS_UPDATES from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_COLUMNS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_BASETYPE_NAME() throws Exception {
		execute("select  BASETYPE_NAME from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_BASETYPE_URL() throws Exception {
		execute("select  BASETYPE_URL from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_IS_AUTO_INCREMENTED()
			throws Exception {
		execute("select  IS_AUTO_INCREMENTED from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_IS_BUILTIN() throws Exception {
		execute("select  IS_BUILTIN from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_IS_CASE_SENSITIVE()
			throws Exception {
		execute("select  IS_CASE_SENSITIVE from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_IS_SIGNED() throws Exception {
		execute("select  IS_SIGNED from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_JAVA_CLASS_NAME() throws Exception {
		execute("select  JAVA_CLASS_NAME from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_LENGTH() throws Exception {
		execute("select  LENGTH from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_NAME_IN_SOURCE() throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_NULL_TYPE() throws Exception {
		execute("select  NULL_TYPE from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_PRECISION() throws Exception {
		execute("select  PRECISION from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_RADIX() throws Exception {
		execute("select  RADIX from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_RUNTIME_TYPE_NAME()
			throws Exception {
		execute("select  RUNTIME_TYPE_NAME from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_SCALE() throws Exception {
		execute("select  SCALE from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_SEARCH_TYPE() throws Exception {
		execute("select  SEARCH_TYPE from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_TYPE() throws Exception {
		execute("select  TYPE from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_URL() throws Exception {
		execute("select  URL from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPES_VARIETY() throws Exception {
		execute("select  VARIETY from SystemPhysical.DATATYPES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPE_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.DATATYPE_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPE_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.DATATYPE_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPE_VARIETY_ENUM_CODE()
			throws Exception {
		execute("select  CODE from SystemPhysical.DATATYPE_VARIETY_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DATATYPE_VARIETY_ENUM_NAME()
			throws Exception {
		execute("select  NAME from SystemPhysical.DATATYPE_VARIETY_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DELETE_TRANS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.DELETE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DELETE_TRANS_TRANSFORMATION()
			throws Exception {
		execute("select  TRANSFORMATION from SystemPhysical.DELETE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DELETE_TRANS_TRANSFORMED_PATH()
			throws Exception {
		execute("select  TRANSFORMED_PATH from SystemPhysical.DELETE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_DELETE_TRANS_TRANSFORMED_UUID()
			throws Exception {
		execute("select  TRANSFORMED_UUID from SystemPhysical.DELETE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_NAME_IN_SOURCE()
			throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_TABLE_NAME() throws Exception {
		execute("select  TABLE_NAME from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_UUID_OF_PRIMARY_KEY()
			throws Exception {
		execute("select  UUID_OF_PRIMARY_KEY from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_UUID_OF_TABLE()
			throws Exception {
		execute("select  UUID_OF_TABLE from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_FOREIGN_KEYS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.FOREIGN_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_NAME_IN_SOURCE() throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_TABLE_NAME() throws Exception {
		execute("select  TABLE_NAME from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INDEXES_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.INDEXES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INSERT_TRANS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.INSERT_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INSERT_TRANS_TRANSFORMATION()
			throws Exception {
		execute("select  TRANSFORMATION from SystemPhysical.INSERT_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INSERT_TRANS_TRANSFORMED_PATH()
			throws Exception {
		execute("select  TRANSFORMED_PATH from SystemPhysical.INSERT_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_INSERT_TRANS_TRANSFORMED_UUID()
			throws Exception {
		execute("select  TRANSFORMED_UUID from SystemPhysical.INSERT_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_KEY_COLUMNS_COLUMN_UUID() throws Exception {
		execute("select  COLUMN_UUID from SystemPhysical.KEY_COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_KEY_COLUMNS_KEY_TYPE() throws Exception {
		execute("select  KEY_TYPE from SystemPhysical.KEY_COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_KEY_COLUMNS_KEY_UUID() throws Exception {
		execute("select  KEY_UUID from SystemPhysical.KEY_COLUMNS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_KEY_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.KEY_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_KEY_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.KEY_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_IS_PHYSICAL() throws Exception {
		execute("select  IS_PHYSICAL from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_IS_VISIBLE() throws Exception {
		execute("select  IS_VISIBLE from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_MAX_SET_SIZE() throws Exception {
		execute("select  MAX_SET_SIZE from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_MODEL_TYPE() throws Exception {
		execute("select  MODEL_TYPE from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_NAME_IN_SOURCE() throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_SUPPORTS_DISTINCT() throws Exception {
		execute("select  SUPPORTS_DISTINCT from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_SUPPORTS_JOIN() throws Exception {
		execute("select  SUPPORTS_JOIN from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_SUPPORTS_ORDER_BY() throws Exception {
		execute("select  SUPPORTS_ORDER_BY from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_SUPPORTS_OUTER_JOIN()
			throws Exception {
		execute("select  SUPPORTS_OUTER_JOIN from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_SUPPORTS_WHERE_ALL() throws Exception {
		execute("select  SUPPORTS_WHERE_ALL from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODELS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.MODELS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODEL_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.MODEL_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_MODEL_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.MODEL_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_NULL_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.NULL_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_NULL_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.NULL_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_NAME_IN_SOURCE()
			throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_TABLE_NAME() throws Exception {
		execute("select  TABLE_NAME from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_UUID_OF_TABLE()
			throws Exception {
		execute("select  UUID_OF_TABLE from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PRIMARY_KEYS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.PRIMARY_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_FUNCTION() throws Exception {
		execute("select  FUNCTION from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_NAME_IN_SOURCE() throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_PROC_TYPE() throws Exception {
		execute("select  PROC_TYPE from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_RESULT_SET_UUID() throws Exception {
		execute("select  RESULT_SET_UUID from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROCS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.PROCS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_DATATYPE_UUID() throws Exception {
		execute("select  DATATYPE_UUID from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_DIRECTION() throws Exception {
		execute("select  DIRECTION from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_IS_OPTIONAL() throws Exception {
		execute("select  IS_OPTIONAL from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_LENGTH() throws Exception {
		execute("select  LENGTH from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_NAME_IN_SOURCE()
			throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_NULL_TYPE() throws Exception {
		execute("select  NULL_TYPE from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_POSITION() throws Exception {
		execute("select  POSITION from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_PRECISION() throws Exception {
		execute("select  PRECISION from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_PROC_NAME() throws Exception {
		execute("select  PROC_NAME from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_PROC_UUID() throws Exception {
		execute("select  PROC_UUID from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_RADIX() throws Exception {
		execute("select  RADIX from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_RUNTIME_TYPE_NAME()
			throws Exception {
		execute("select  RUNTIME_TYPE_NAME from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_SCALE() throws Exception {
		execute("select  SCALE from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAMS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.PROC_PARAMS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAM_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.PROC_PARAM_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_PARAM_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.PROC_PARAM_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_MODEL_NAME()
			throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_NAME_IN_SOURCE()
			throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_OBJECT_TYPE()
			throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_PROC_NAME()
			throws Exception {
		execute("select  PROC_NAME from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_PROC_UUID()
			throws Exception {
		execute("select  PROC_UUID from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_RESULT_SETS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.PROC_RESULT_SETS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_TRANS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.PROC_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_TRANS_TRANSFORMATION() throws Exception {
		execute("select  TRANSFORMATION from SystemPhysical.PROC_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_TRANS_TRANSFORMED_PATH()
			throws Exception {
		execute("select  TRANSFORMED_PATH from SystemPhysical.PROC_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_TRANS_TRANSFORMED_UUID()
			throws Exception {
		execute("select  TRANSFORMED_UUID from SystemPhysical.PROC_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.PROC_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROC_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.PROC_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROPERTIES_PROPERTIED_UUID()
			throws Exception {
		execute("select  PROPERTIED_UUID from SystemPhysical.PROPERTIES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROPERTIES_PROP_NAME() throws Exception {
		execute("select  PROP_NAME from SystemPhysical.PROPERTIES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_PROPERTIES_PROP_VALUE() throws Exception {
		execute("select  PROP_VALUE from SystemPhysical.PROPERTIES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_SEARCH_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.SEARCH_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_SEARCH_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.SEARCH_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void test_SystemPhysical_SELECT_TRANS_OBJECT_TYPE() throws Exception {
		execute("select OBJECT_TYPE from SystemPhysical.SELECT_TRANS"); //$NON-NLS-1$
	}

	public void test_SystemPhysical_SELECT_TRANS_TRANSFORMATION()
			throws Exception {
		execute("select TRANSFORMATION from SystemPhysical.SELECT_TRANS"); //$NON-NLS-1$
	}

	public void test_SystemPhysical_SELECT_TRANS_TRANSFORMED_PATH()
			throws Exception {
		execute("select TRANSFORMED_PATH from SystemPhysical.SELECT_TRANS"); //$NON-NLS-1$
	}

	public void test_SystemPhysical_SELECT_TRANS_TRANSFORMED_UUID()
			throws Exception {
		execute("select TRANSFORMED_UUID from SystemPhysical.SELECT_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_CARDINALITY() throws Exception {
		execute("select  CARDINALITY from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_IS_PHYSICAL() throws Exception {
		execute("select  IS_PHYSICAL from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_IS_SYSTEM() throws Exception {
		execute("select  IS_SYSTEM from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_IS_VIRTUAL() throws Exception {
		execute("select  IS_VIRTUAL from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_NAME_IN_SOURCE() throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_SUPPORTS_UPDATE() throws Exception {
		execute("select  SUPPORTS_UPDATE from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_TABLE_TYPE() throws Exception {
		execute("select  TABLE_TYPE from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLES_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.TABLES"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLE_TYPE_ENUM_CODE() throws Exception {
		execute("select  CODE from SystemPhysical.TABLE_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_TABLE_TYPE_ENUM_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.TABLE_TYPE_ENUM"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_FULLNAME() throws Exception {
		execute("select  FULLNAME from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_MODEL_NAME() throws Exception {
		execute("select  MODEL_NAME from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_NAME_IN_SOURCE()
			throws Exception {
		execute("select  NAME_IN_SOURCE from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_NAME() throws Exception {
		execute("select  NAME from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_PATH() throws Exception {
		execute("select  PATH from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_TABLE_NAME() throws Exception {
		execute("select  TABLE_NAME from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_UUID_OF_TABLE() throws Exception {
		execute("select  UUID_OF_TABLE from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UNIQUE_KEYS_UUID() throws Exception {
		execute("select  UUID from SystemPhysical.UNIQUE_KEYS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UPDATE_TRANS_OBJECT_TYPE() throws Exception {
		execute("select  OBJECT_TYPE from SystemPhysical.UPDATE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UPDATE_TRANS_TRANSFORMATION()
			throws Exception {
		execute("select  TRANSFORMATION from SystemPhysical.UPDATE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UPDATE_TRANS_TRANSFORMED_PATH()
			throws Exception {
		execute("select  TRANSFORMED_PATH from SystemPhysical.UPDATE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_UPDATE_TRANS_TRANSFORMED_UUID()
			throws Exception {
		execute("select  TRANSFORMED_UUID from SystemPhysical.UPDATE_TRANS"); //$NON-NLS-1$
	}

	public void testSystemPhysical_VDB_INFO_NAME() throws Exception {
		String[] expected = { "System" }; //$NON-NLS-1$
		execute("select  NAME from SystemPhysical.VDB_INFO", expected); //$NON-NLS-1$
	}

	public void testSystemPhysical_VDB_INFO_VERSION() throws Exception {
		String[] expected = { "1" }; //$NON-NLS-1$
		execute("select  VERSION from SystemPhysical.VDB_INFO", expected); //$NON-NLS-1$
	}

	public void testRepeatedQueries() throws Exception {
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
		execute("select  parent_name, name as n from columns order by n"); //$NON-NLS-1$
	}

}
