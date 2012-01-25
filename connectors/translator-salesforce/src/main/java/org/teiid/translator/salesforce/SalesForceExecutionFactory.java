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

package org.teiid.translator.salesforce;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.util.Arrays;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.salesforce.execution.DeleteExecutionImpl;
import org.teiid.translator.salesforce.execution.InsertExecutionImpl;
import org.teiid.translator.salesforce.execution.ProcedureExecutionParentImpl;
import org.teiid.translator.salesforce.execution.QueryExecutionImpl;
import org.teiid.translator.salesforce.execution.UpdateExecutionImpl;

@Translator(name="salesforce", description="A translator for Salesforce")
public class SalesForceExecutionFactory extends ExecutionFactory<ConnectionFactory, SalesforceConnection> {

	private static final String SALESFORCE = "salesforce"; //$NON-NLS-1$
	private static final String EXCLUDES = "excludes";//$NON-NLS-1$
	private static final String INCLUDES = "includes";//$NON-NLS-1$
	private boolean auditModelFields = false;
	
	public SalesForceExecutionFactory() {
	    // http://jira.jboss.org/jira/browse/JBEDSP-306
	    // Salesforce supports ORDER BY, but not on all column types
		setSupportsOrderBy(false);
		setSupportsOuterJoins(true);
		setSupportedJoinCriteria(SupportedJoinCriteria.KEY);
	}
	
	@TranslatorProperty(display="Audit Model Fields", advanced=true)
	public boolean isModelAuditFields() {
		return this.auditModelFields;
	}
	
	public void setModelAuditFields(boolean modelAuditFields) {
		this.auditModelFields = modelAuditFields;
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		addPushDownFunction(SALESFORCE, INCLUDES, BOOLEAN, STRING, STRING);
		addPushDownFunction(SALESFORCE, EXCLUDES, BOOLEAN, STRING, STRING);
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Salesforce ExecutionFactory Started"); //$NON-NLS-1$
	}


	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection)
			throws TranslatorException {
		return new QueryExecutionImpl(command, connection, metadata, executionContext);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection) throws TranslatorException {
		UpdateExecution result = null;
		if(command instanceof org.teiid.language.Delete) {
			result = new DeleteExecutionImpl(command, connection, metadata, executionContext);
		} else if (command instanceof org.teiid.language.Insert) {
			result = new InsertExecutionImpl(command, connection, metadata, executionContext);
		} else if (command instanceof org.teiid.language.Update) {
			result = new UpdateExecutionImpl(command, connection, metadata, executionContext);
		}
		return result;

	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection)
			throws TranslatorException {
		return new ProcedureExecutionParentImpl(command, connection, metadata, executionContext);
	}
	@Override
	public void getMetadata(MetadataFactory metadataFactory, SalesforceConnection connection) throws TranslatorException {
		MetadataProcessor processor = new MetadataProcessor(connection,metadataFactory, this);
		processor.processMetadata();
	}	
	
    @Override
    public List<String> getSupportedFunctions() {
        return Arrays.asList(INCLUDES, EXCLUDES);
    }
    
    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }
    
    @Override
    public boolean supportsAggregatesCount() {
    	return true;
    }
    
    @Override
    public boolean supportsAggregatesMax() {
    	return true;
    }

    @Override
    public boolean supportsAggregatesMin() {
    	return true;
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
    public boolean supportsGroupBy() {
    	return true;
    }
    
    @Override
    public boolean supportsOnlySingleTableGroupBy() {
    	return true;
    }

    @Override
    public boolean supportsNotCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return true;
    }
    
    @Override
    public boolean supportsIsNullCriteria() {
    	return true;
    }
    
    @Override
    public boolean supportsHaving() {
    	return true;
    }
    
    @Override
    public int getMaxFromGroups() {
    	return 2;
    }
    
    @Override
    public boolean useAnsiJoin() {
    	return true;
    }
    
}
