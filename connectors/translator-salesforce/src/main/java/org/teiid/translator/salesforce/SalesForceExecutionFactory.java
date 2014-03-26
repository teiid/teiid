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

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BOOLEAN;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;

import java.util.Arrays;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.salesforce.execution.*;

@Translator(name="salesforce", description="A translator for Salesforce")
public class SalesForceExecutionFactory extends ExecutionFactory<ConnectionFactory, SalesforceConnection> {
	private static final String SALESFORCE = "salesforce"; //$NON-NLS-1$
	private static final String EXCLUDES = "excludes";//$NON-NLS-1$
	private static final String INCLUDES = "includes";//$NON-NLS-1$
	private boolean auditModelFields = false;
	private int maxInsertBatchSize = 2048;
	
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
			result = new DeleteExecutionImpl(this, command, connection, metadata, executionContext);
		} else if (command instanceof org.teiid.language.Insert) {
			result = new InsertExecutionImpl(this, command, connection, metadata, executionContext);
		} else if (command instanceof org.teiid.language.Update) {
			result = new UpdateExecutionImpl(this, command, connection, metadata, executionContext);
		}
		return result;

	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection)
			throws TranslatorException {
		Procedure metadataObject = command.getMetadataObject();
		String nativeQuery = metadataObject.getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
		if (nativeQuery != null) {
			return new DirectQueryExecution(command.getArguments(), command, connection, metadata, executionContext, nativeQuery, false);
    	}
		return new ProcedureExecutionParentImpl(command, connection, metadata, executionContext);
	}

	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection) throws TranslatorException {
		 return new DirectQueryExecution(arguments.subList(1, arguments.size()), command, connection, metadata, executionContext, (String)arguments.get(0).getArgumentValue().getValue(), true);
	}	
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, SalesforceConnection connection) throws TranslatorException {
	    metadataFactory.getModelProperties().setProperty("importer.modelAuditFields", String.valueOf(this.auditModelFields)); //$NON-NLS-1$
	    super.getMetadata(metadataFactory, connection);	    
	}	
	
	@Override
    public MetadataProcessor<SalesforceConnection> getMetadataProcessor(){
	    return new SalesForceMetadataProcessor();
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
    public boolean supportsOnlyLiteralComparison() {
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
    
    @Override
    public boolean supportsBulkUpdate() {
    	return true;
    }    
    
    @TranslatorProperty(display="Max Bulk Insert Batch Size", description="The max size of a bulk insert batch.  Default 2048.", advanced=true)
    public int getMaxBulkInsertBatchSize() {
    	return maxInsertBatchSize;
    }
    
    public void setMaxBulkInsertBatchSize(int maxInsertBatchSize) {
    	if (maxInsertBatchSize < 1) {
    		throw new AssertionError("Max bulk insert batch size must be greater than 0"); //$NON-NLS-1$
    	}
		this.maxInsertBatchSize = maxInsertBatchSize;
	}
}
