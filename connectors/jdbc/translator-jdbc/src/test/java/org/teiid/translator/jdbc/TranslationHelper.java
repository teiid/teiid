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

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.function.metadata.FunctionMetadataReader;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TranslationHelper {
	
    public static final String PARTS_VDB = "/PartsSupplier.vdb"; //$NON-NLS-1$
    public static final String BQT_VDB = "/bqt.vdb"; //$NON-NLS-1$

    public static Command helpTranslate(String vdbFileName, String sql) {
    	return helpTranslate(vdbFileName, null, null, sql);
    }
    
    public static TranslationUtility getTranslationUtility(String vdbFileName, String udf) {
    	TranslationUtility util = null;
    	if (PARTS_VDB.equals(vdbFileName)) {
    		util = new TranslationUtility("PartsSupplier.vdb", TranslationHelper.class.getResource(vdbFileName)); //$NON-NLS-1$
    	} else if (BQT_VDB.equals(vdbFileName)){
    		util = FakeTranslationFactory.getInstance().getBQTTranslationUtility();
    	} else {
    		try {
				util = new TranslationUtility(RealMetadataFactory.fromDDL(vdbFileName, "vdb", "test"));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
    	}
    	
    	if (udf != null) {
    		loadUDFs(udf, util);
    	}
    	return util;
    }

	public static void loadUDFs(String udf, TranslationUtility util) {
		try {
			Collection <FunctionMethod> methods = FunctionMetadataReader.loadFunctionMethods(TranslationHelper.class.getResource(udf).openStream());
			util.addUDF("foo", methods); //$NON-NLS-1$
		} catch (IOException e) {
			throw new TeiidRuntimeException("failed to load UDF"); //$NON-NLS-1$
		} catch (XMLStreamException e) {
			throw new TeiidRuntimeException("failed to load UDF"); //$NON-NLS-1$
		}
	}
    
    public static Command helpTranslate(String vdbFileName, String udf, List<FunctionMethod> pushdowns, String sql) {
    	TranslationUtility util =  getTranslationUtility(vdbFileName, null);   
    	
    	if (pushdowns != null) {
    		util.addUDF(CoreConstants.SYSTEM_MODEL, pushdowns);
    	}
    	if (udf != null) {
        	Collection <FunctionMethod> methods = new ArrayList<FunctionMethod>();
    		try {
				methods.addAll(FunctionMetadataReader.loadFunctionMethods(TranslationHelper.class.getResource(udf).openStream()));
			} catch (XMLStreamException e) {
				throw new TeiidRuntimeException("failed to load UDF"); //$NON-NLS-1$
			} catch (IOException e) {
				throw new TeiidRuntimeException("failed to load UDF"); //$NON-NLS-1$
			}
			util.addUDF("foo", methods); //$NON-NLS-1$
    	}
    	return util.parseCommand(sql);
    }    

	public static TranslatedCommand helpTestVisitor(String vdb, String input, String expectedOutput, JDBCExecutionFactory translator) throws TranslatorException {
		return helpTestVisitor(vdb,null,input, expectedOutput, translator);
	}
	
	public static TranslatedCommand helpTestVisitor(String vdb, String udf, String input, String expectedOutput, JDBCExecutionFactory translator) throws TranslatorException {
	    // Convert from sql to objects
	    Command obj = helpTranslate(vdb, udf, translator.getPushDownFunctions(), input);
	    
	    return helpTestVisitor(expectedOutput, translator, obj);
	}	

	public static TranslatedCommand helpTestVisitor(String expectedOutput, JDBCExecutionFactory translator, Command obj) throws TranslatorException {
		TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), translator);
	    tc.translateCommand(obj);
	    assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
	    return tc;
	}
	
	public static String helpTestTempTable(JDBCExecutionFactory transaltor, boolean transactional) throws QueryMetadataException, TeiidComponentException {
		List<ColumnReference> cols = new ArrayList<ColumnReference>();
		cols.add(new ColumnReference(null, "COL1", RealMetadataFactory.exampleBQTCached().getElementID("BQT1.SMALLA.INTKEY"), TypeFacility.RUNTIME_TYPES.INTEGER));
		cols.add(new ColumnReference(null, "COL2", RealMetadataFactory.exampleBQTCached().getElementID("BQT1.SMALLA.STRINGKEY"), TypeFacility.RUNTIME_TYPES.STRING));
		return transaltor.getCreateTempTableSQL("foo", cols, transactional);
	}

}
