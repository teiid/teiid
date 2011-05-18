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

package org.teiid.coherence.translator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;


import org.teiid.coherence.connector.CoherenceConnection;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;


@Translator(name="coherence", description="A translator for testing Coherence")
public class CoherenceExecutionFactory extends ExecutionFactory<ConnectionFactory, CoherenceConnection> {


	public static final int MAX_SET_SIZE = 100;
	
    @Override
    public void start() throws TranslatorException {
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, CoherenceConnection connection)
    		throws TranslatorException {
    	return new CoherenceExecution((Select)command, metadata, connection);
    }    
    
    public List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }

    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    public boolean supportsInCriteria() {
        return true;
    }

    public int getMaxInCriteriaSize() {
        return MAX_SET_SIZE;
    }
    
    @Override
    public boolean isSourceRequired() {
    	return false;
    }
    
    @Override
    public void getMetadata(MetadataFactory metadataFactory,
    		CoherenceConnection conn) throws TranslatorException {
		Table t = metadataFactory.addTable("Trade"); //$NON-NLS-1$
		metadataFactory.addColumn("tradeid", DataTypeManager.DefaultDataTypes.LONG, t); //$NON-NLS-1$
		metadataFactory.addColumn("legid", DataTypeManager.DefaultDataTypes.LONG, t); //$NON-NLS-1$
		metadataFactory.addColumn("notational", DataTypeManager.DefaultDataTypes.DOUBLE, t); //$NON-NLS-1$
    }
  
  
}
