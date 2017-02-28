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
package org.teiid.translator.couchbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.types.DataTypeManager;
import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.Function;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;


@Translator(name="couchbase", description="Couchbase Translator, reads and writes the data to Couchbase")
public class CouchbaseExecutionFactory extends ExecutionFactory<ConnectionFactory, CouchbaseConnection> {
    
//    private class CouchbaseConvertModifier FunctionModifier
    
    private static final String COUCHBASE = "couchbase"; //$NON-NLS-1$
    
    protected Map<String, FunctionModifier> functionModifiers = new TreeMap<String, FunctionModifier>(String.CASE_INSENSITIVE_ORDER);

	public CouchbaseExecutionFactory() {
		setSourceRequiredForMetadata(false);
		setTransactionSupport(TransactionSupport.NONE);
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		
		registerFunctionModifier(SourceSystemFunctions.CONVERT, new CouchbaseConvertModifier());
		registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("CEIL"));//$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("LN"));//$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("LOG"));//$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.RAND, new AliasModifier("RANDOM"));//$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("LOWER"));//$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPER"));//$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.TRANSLATE, new AliasModifier("REPLACE"));//$NON-NLS-1$
		
		addPushDownFunction(COUCHBASE, "CONTAINS", TypeFacility.RUNTIME_NAMES.BOOLEAN, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(COUCHBASE, "TITLE", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(COUCHBASE, "LTRIM", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(COUCHBASE, "TRIM", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(COUCHBASE, "RTRIM", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(COUCHBASE, "POSITION", TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(COUCHBASE, "REVERSE", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
	}

	@Override
    public List<String> getSupportedFunctions() {
	    
	    List<String> supportedFunctions = new ArrayList<String>();
	    
	    // Numeric
	    supportedFunctions.addAll(getDefaultSupportedFunctions());
	    supportedFunctions.add(SourceSystemFunctions.ABS);
	    supportedFunctions.add(SourceSystemFunctions.ACOS);
	    supportedFunctions.add(SourceSystemFunctions.ASIN);
	    supportedFunctions.add(SourceSystemFunctions.ATAN);
	    supportedFunctions.add(SourceSystemFunctions.ATAN2);
	    supportedFunctions.add(SourceSystemFunctions.CEILING);
	    supportedFunctions.add(SourceSystemFunctions.COS);
	    supportedFunctions.add(SourceSystemFunctions.DEGREES);
	    supportedFunctions.add(SourceSystemFunctions.EXP);
	    supportedFunctions.add(SourceSystemFunctions.FLOOR);
	    supportedFunctions.add(SourceSystemFunctions.LOG);
	    supportedFunctions.add(SourceSystemFunctions.LOG10);
	    supportedFunctions.add(SourceSystemFunctions.PI);
	    supportedFunctions.add(SourceSystemFunctions.POWER);
	    supportedFunctions.add(SourceSystemFunctions.RADIANS);
	    supportedFunctions.add(SourceSystemFunctions.RAND);
	    supportedFunctions.add(SourceSystemFunctions.ROUND);
	    supportedFunctions.add(SourceSystemFunctions.SIGN);
	    supportedFunctions.add(SourceSystemFunctions.SIN);
	    supportedFunctions.add(SourceSystemFunctions.SQRT);
	    supportedFunctions.add(SourceSystemFunctions.TAN);
	    
	    //String
	    supportedFunctions.add(SourceSystemFunctions.TAN);
	    supportedFunctions.add(SourceSystemFunctions.INITCAP);
	    supportedFunctions.add(SourceSystemFunctions.LENGTH);
	    supportedFunctions.add(SourceSystemFunctions.LCASE);
	    supportedFunctions.add(SourceSystemFunctions.REPEAT);
	    supportedFunctions.add(SourceSystemFunctions.TRANSLATE);
	    supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
	    supportedFunctions.add(SourceSystemFunctions.UCASE);
	    
        return supportedFunctions;
    }
	
	public void registerFunctionModifier(String name, FunctionModifier modifier) {
        this.functionModifiers.put(name, modifier);
    }

    public Map<String, FunctionModifier> getFunctionModifiers() {
        return this.functionModifiers;
    }
	
	public List<String> getDefaultSupportedFunctions(){
        return Arrays.asList(new String[] { "+", "-", "*", "/" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) throws TranslatorException {
		return new CouchbaseQueryExecution(this, command, executionContext, metadata, connection);
	}

    @Override
    public MetadataProcessor<CouchbaseConnection> getMetadataProcessor() {
        return new CouchbaseMetadataProcessor();
    }

    @Override
    public boolean supportsAggregatesSum() {
        return true;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return false;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
        return true;
    }

    @Override
    public boolean supportsArrayAgg() {
        return true;
    }
    
    public N1QLVisitor getN1QLVisitor(RuntimeMetadata metadata) {
        return new N1QLVisitor(this, metadata);
    }

}
