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
package org.teiid.translator.accumulo;

import java.nio.charset.Charset;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;

@Translator(name="accumulo", description="Accumulo Translator, reads and writes the data to Accumulo Key/Value store")
public class AccumuloExecutionFactory extends ExecutionFactory<ConnectionFactory, AccumuloConnection> {
	private int queryThreadsCount = 10;
	private Charset encoding = Charset.defaultCharset();
	
	public AccumuloExecutionFactory() {
		setSourceRequiredForMetadata(true);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
	}

	@TranslatorProperty(display="Execution Query Threads", description="Number of threads to use on Accumulo for Query", advanced=true)
	public int getQueryThreadsCount() {
		return queryThreadsCount;
	}

	public void setQueryThreadsCount(int queryThreadsCount) {
		this.queryThreadsCount = queryThreadsCount;
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			AccumuloConnection connection) throws TranslatorException {
		return new AccumuloQueryExecution(this, (Select) command, executionContext, metadata, connection);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, AccumuloConnection connection) throws TranslatorException {
		return new AccumuloUpdateExecution(this, command, executionContext, metadata, connection);
	} 	
		
	@Override
    public MetadataProcessor<AccumuloConnection> getMetadataProcessor() {
        return new AccumuloMetadataProcessor();
    }	
	
	@TranslatorProperty(display="Encoding", description="Character Encoding to use for reading and saving text based data", advanced=true)
	public String getEncoding() {
		return this.encoding.name();
	}
	
	Charset getChasetEncoding() {
		return this.encoding;
	}	

	public void setEncoding(String encoding) {
		this.encoding = Charset.forName(encoding);
	}	
	
	@Override
	public boolean supportsAggregatesCountStar() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaEquals() {
		return true;
	}
	
	@Override
	public boolean supportsNotCriteria() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}

	@Override
	public boolean supportsInCriteria() {
		return true;
	}	
	
	@Override
	public boolean supportsOnlyLiteralComparison() {
		return true;
	}	

    @Override
	public boolean supportsIsNullCriteria() {
    	return false;
    }	
    
    @Override
	public boolean supportsOrCriteria() {
    	return true;
    }
	
    @Override
	public boolean supportsBulkUpdate() {
		return true;
	}    
}
