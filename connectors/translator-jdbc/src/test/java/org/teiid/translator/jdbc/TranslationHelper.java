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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.bind.JAXBException;

import junit.framework.Assert;

import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.Command;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.function.metadata.FunctionMetadataReader;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ExecutionContext;


public class TranslationHelper {
	
    public static final String PARTS_VDB = "/PartsSupplier.vdb"; //$NON-NLS-1$
    public static final String BQT_VDB = "/bqt.vdb"; //$NON-NLS-1$

    public static Command helpTranslate(String vdbFileName, String sql) {
    	return helpTranslate(vdbFileName, null, null, sql);
    }
    
    public static TranslationUtility getTranslationUtility(String vdbFileName, String udf) {
    	TranslationUtility util = null;
    	if (PARTS_VDB.equals(vdbFileName)) {
    		util = new TranslationUtility(TranslationHelper.class.getResource(vdbFileName));
    	} else if (BQT_VDB.equals(vdbFileName)){
    		util = FakeTranslationFactory.getInstance().getBQTTranslationUtility();
    	} else {
    		Assert.fail("unknown vdb"); //$NON-NLS-1$
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
		} catch (JAXBException e) {
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
			} catch (JAXBException e) {
				throw new TeiidRuntimeException("failed to load UDF"); //$NON-NLS-1$
			} catch (IOException e) {
				throw new TeiidRuntimeException("failed to load UDF"); //$NON-NLS-1$
			}
			util.addUDF("foo", methods); //$NON-NLS-1$
    	}
    	return util.parseCommand(sql);
    }    

	public static void helpTestVisitor(String vdb, String input, String expectedOutput, JDBCExecutionFactory translator) throws TranslatorException {
		helpTestVisitor(vdb,null,input, expectedOutput, translator);
	}
	
	public static void helpTestVisitor(String vdb, String udf, String input, String expectedOutput, JDBCExecutionFactory translator) throws TranslatorException {
	    // Convert from sql to objects
	    Command obj = helpTranslate(vdb, udf, translator.getPushDownFunctions(), input);
	    
	    helpTestVisitor(expectedOutput, translator, obj);
	}	

	public static void helpTestVisitor(String expectedOutput, JDBCExecutionFactory translator, Command obj) throws TranslatorException {
		TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), translator);
	    tc.translateCommand(obj);
	    assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
	}

}
