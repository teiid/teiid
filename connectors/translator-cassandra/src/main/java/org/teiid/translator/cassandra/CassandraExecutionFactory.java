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

package org.teiid.translator.cassandra;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.BundleUtil;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.cassandra.execution.CassandraQueryExecution;
import org.teiid.translator.cassandra.execution.CassandraUpdateExecution;
import org.teiid.translator.cassandra.metadata.CassandraMetadataProcessor;


@Translator(name = "cassandra", description = "A translator for Cassandra NoSql database")
public class CassandraExecutionFactory extends ExecutionFactory<ConnectionFactory, CassandraConnection> {
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CassandraExecutionFactory.class);

	@Override
	public void start() throws TranslatorException {
		super.start();
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Cassandra ExecutionFactory Started"); //$NON-NLS-1$
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			CassandraConnection connection) throws TranslatorException {
		return new CassandraQueryExecution((Select) command, connection, executionContext);
	}

	@Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			CassandraConnection connection) throws TranslatorException {
		return new CassandraUpdateExecution(command, executionContext, metadata, connection);
	} 	
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory,
			CassandraConnection conn) throws TranslatorException {
			CassandraMetadataProcessor processor = new CassandraMetadataProcessor(metadataFactory, conn.keyspaceInfo());
			processor.processMetadata();
	}

	@Override
	public boolean supportsOrderBy() {
		return true;
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
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}

	@Override
	public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean supportsRowLimit() {
		return true;
	}
	
}
