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

package org.teiid.translator.google;

import java.util.Arrays;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.BundleUtil;
import org.teiid.language.*;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.translator.*;

@Translator(name="google-spreadsheet", description="A translator for Google Spreadsheet")
public class SpreadsheetExecutionFactory extends ExecutionFactory<ConnectionFactory, GoogleSpreadsheetConnection>{
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(SpreadsheetExecutionFactory.class);
	public SpreadsheetExecutionFactory() {
	    setSourceRequiredForMetadata(false);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Google Spreadsheet ExecutionFactory Started"); //$NON-NLS-1$
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, GoogleSpreadsheetConnection connection)
			throws TranslatorException {
		return new SpreadsheetQueryExecution((Select)command, connection, executionContext);
	}

	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, GoogleSpreadsheetConnection connection) throws TranslatorException {
		 return new DirectSpreadsheetQueryExecution((String)arguments.get(0).getArgumentValue().getValue(), arguments.subList(1, arguments.size()), executionContext, connection, true);
	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			GoogleSpreadsheetConnection connection) throws TranslatorException {
		String nativeQuery = command.getMetadataObject().getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
		if (nativeQuery != null) {
			return new DirectSpreadsheetQueryExecution(nativeQuery, command.getArguments(), executionContext, connection, false);
		}
		throw new TranslatorException("Missing native-query extension metadata."); //$NON-NLS-1$
	}
	
	@Override
    public MetadataProcessor<GoogleSpreadsheetConnection> getMetadataProcessor(){
	    return new GoogleMetadataProcessor();
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
	public boolean supportsOrCriteria() {
		return true;
	}

	@Override
	public boolean supportsNotCriteria() {
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
	public boolean supportsOrderBy() {
		return false;
	}

	@Override
	public boolean supportsHaving() {
		return false;
	}

	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}

	@Override
	public boolean supportsRowLimit() {
		return true;
	}

	@Override
	public boolean supportsRowOffset() {
		return true;
	}

	@Override
	public List<String> getSupportedFunctions() {
		return Arrays.asList(SourceSystemFunctions.YEAR,
				SourceSystemFunctions.MONTH, SourceSystemFunctions.DAYOFMONTH,
				SourceSystemFunctions.HOUR, SourceSystemFunctions.MINUTE,
				SourceSystemFunctions.SECOND, SourceSystemFunctions.QUARTER,
				SourceSystemFunctions.DAYOFWEEK, SourceSystemFunctions.UCASE,
				SourceSystemFunctions.LCASE);
	}
	
}
