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

package org.teiid.translator;

import java.util.List;

import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.LanguageFactory;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;

/**
 * Delegate translator. User can define a {@link ExecutionFactory} of their own and have this translator 
 * delegate all the calls to that class. Please note that your 'vdb.xml' file will contain
 * an xml fragment like the following to configure a delegating translator.
 * <pre>
 * {@code
    <translator type="delegate" name="my-translator" description="custom translator">
        <property value="delegateName" name="name of the delegate instance"/>
    </translator>
   }
 * </pre>
 *  
 */
public class BaseDelegatingExecutionFactory<F, C> extends ExecutionFactory<F, C> implements DelegatingExecutionFactory<F, C> {

	private String delegateName;
	private ExecutionFactory<F, C> delegate;
	
	/**
	 * For testing only
	 */
	ExecutionFactory<F, C> getDelegate() {
		return this.delegate;
	}
	
	public void setDelegate(ExecutionFactory<F, C> delegate) {
		this.delegate = delegate;
	}
	
	@TranslatorProperty(display="Delegate name", required = true)
	public String getDelegateName() {
		return this.delegateName;
	}
	
	public void setDelegateName(String delegateName) {
		this.delegateName = delegateName;
	}
	
	@Override
	public void start() throws TranslatorException {
		this.delegate.start();
	}
	
	@Override
	public boolean areLobsUsableAfterClose() {
		return delegate.areLobsUsableAfterClose();
	}
	@Override
	public void closeConnection(C connection, F factory) {
		delegate.closeConnection(connection, factory);
	}
	@Override
	public Execution createExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			C connection) throws TranslatorException {
		return delegate.createExecution(command, executionContext, metadata,
				connection);
	}
	@Override
	public ProcedureExecution createProcedureExecution(Call command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			C connection) throws TranslatorException {
		return delegate.createProcedureExecution(command, executionContext,
				metadata, connection);
	}
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			C connection) throws TranslatorException {
		return delegate.createResultSetExecution(command, executionContext,
				metadata, connection);
	}
	@Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			C connection) throws TranslatorException {
		return delegate.createUpdateExecution(command, executionContext,
				metadata, connection);
	}
	@Override
	public C getConnection(F factory) throws TranslatorException {
		return delegate.getConnection(factory);
	}
	@Override
	public NullOrder getDefaultNullOrder() {
		return delegate.getDefaultNullOrder();
	}
	@Override
	public LanguageFactory getLanguageFactory() {
		return delegate.getLanguageFactory();
	}
	@Override
	public int getMaxFromGroups() {
		return delegate.getMaxFromGroups();
	}
	@Override
	public void getMetadata(MetadataFactory metadataFactory, C conn)
			throws TranslatorException {
		delegate.getMetadata(metadataFactory, conn);
	}
	@Override
	public List<FunctionMethod> getPushDownFunctions() {
		return delegate.getPushDownFunctions();
	}
	@Override
	public List<String> getSupportedFunctions() {
		return delegate.getSupportedFunctions();
	}
	@Override
	public TypeFacility getTypeFacility() {
		return delegate.getTypeFacility();
	}
	@Override
	public boolean isImmutable() {
		return delegate.isImmutable();
	}
	@Override
	public boolean isSourceRequired() {
		return delegate.isSourceRequired();
	}
	@Override
	public boolean supportsAggregatesAvg() {
		return delegate.supportsAggregatesAvg();
	}
	@Override
	public boolean supportsAggregatesCount() {
		return delegate.supportsAggregatesCount();
	}
	@Override
	public boolean supportsAggregatesCountStar() {
		return delegate.supportsAggregatesCountStar();
	}
	@Override
	public boolean supportsAggregatesDistinct() {
		return delegate.supportsAggregatesDistinct();
	}
	@Override
	public boolean supportsAggregatesEnhancedNumeric() {
		return delegate.supportsAggregatesEnhancedNumeric();
	}
	@Override
	public boolean supportsAggregatesMax() {
		return delegate.supportsAggregatesMax();
	}
	@Override
	public boolean supportsAggregatesMin() {
		return delegate.supportsAggregatesMin();
	}
	@Override
	public boolean supportsAggregatesSum() {
		return delegate.supportsAggregatesSum();
	}
	@Override
	public boolean supportsAliasedTable() {
		return delegate.supportsAliasedTable();
	}
	@Override
	public boolean supportsBatchedUpdates() {
		return delegate.supportsBatchedUpdates();
	}
	@Override
	public boolean supportsBetweenCriteria() {
		return delegate.supportsBetweenCriteria();
	}
	@Override
	public boolean supportsBulkUpdate() {
		return delegate.supportsBulkUpdate();
	}
	@Override
	public boolean supportsCaseExpressions() {
		return delegate.supportsCaseExpressions();
	}
	@Override
	public boolean supportsCommonTableExpressions() {
		return delegate.supportsCommonTableExpressions();
	}
	@Override
	public boolean supportsCompareCriteriaEquals() {
		return delegate.supportsCompareCriteriaEquals();
	}
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return delegate.supportsCompareCriteriaOrdered();
	}
	@Override
	public boolean supportsCorrelatedSubqueries() {
		return delegate.supportsCorrelatedSubqueries();
	}
	@Override
	public boolean supportsExcept() {
		return delegate.supportsExcept();
	}
	@Override
	public boolean supportsExistsCriteria() {
		return delegate.supportsExistsCriteria();
	}
	@Override
	public boolean supportsFunctionsInGroupBy() {
		return delegate.supportsFunctionsInGroupBy();
	}
	@Override
	public boolean supportsGroupBy() {
		return delegate.supportsGroupBy();
	}
	@Override
	public boolean supportsHaving() {
		return delegate.supportsHaving();
	}
	@Override
	public boolean supportsInCriteria() {
		return delegate.supportsInCriteria();
	}
	@Override
	public boolean supportsInCriteriaSubquery() {
		return delegate.supportsInCriteriaSubquery();
	}
	@Override
	public boolean supportsInlineViews() {
		return delegate.supportsInlineViews();
	}
	@Override
	public boolean supportsInsertWithIterator() {
		return delegate.supportsInsertWithIterator();
	}
	@Override
	public boolean supportsInsertWithQueryExpression() {
		return delegate.supportsInsertWithQueryExpression();
	}
	@Override
	public boolean supportsIntersect() {
		return delegate.supportsIntersect();
	}
	@Override
	public boolean supportsIsNullCriteria() {
		return delegate.supportsIsNullCriteria();
	}
	@Override
	public boolean supportsLikeCriteria() {
		return delegate.supportsLikeCriteria();
	}
	@Override
	public boolean supportsLikeCriteriaEscapeCharacter() {
		return delegate.supportsLikeCriteriaEscapeCharacter();
	}
	@Override
	public boolean supportsNotCriteria() {
		return delegate.supportsNotCriteria();
	}
	@Override
	public boolean supportsOrCriteria() {
		return delegate.supportsOrCriteria();
	}
	@Override
	public boolean supportsOrderByNullOrdering() {
		return delegate.supportsOrderByNullOrdering();
	}
	@Override
	public boolean supportsOrderByUnrelated() {
		return delegate.supportsOrderByUnrelated();
	}
	@Override
	public boolean supportsQuantifiedCompareCriteriaAll() {
		return delegate.supportsQuantifiedCompareCriteriaAll();
	}
	@Override
	public boolean supportsQuantifiedCompareCriteriaSome() {
		return delegate.supportsQuantifiedCompareCriteriaSome();
	}
	@Override
	public boolean supportsRowLimit() {
		return delegate.supportsRowLimit();
	}
	@Override
	public boolean supportsRowOffset() {
		return delegate.supportsRowOffset();
	}
	@Override
	public boolean supportsScalarSubqueries() {
		return delegate.supportsScalarSubqueries();
	}
	@Override
	public boolean supportsSearchedCaseExpressions() {
		return delegate.supportsSearchedCaseExpressions();
	}
	@Override
	public boolean supportsSelectExpression() {
		return delegate.supportsSelectExpression();
	}
	@Override
	public boolean supportsSelfJoins() {
		return delegate.supportsSelfJoins();
	}
	@Override
	public boolean supportsSetQueryOrderBy() {
		return delegate.supportsSetQueryOrderBy();
	}
	@Override
	public boolean supportsUnions() {
		return delegate.supportsUnions();
	}
	@Override
	public String toString() {
		return delegate.toString();
	}
	@Override
	public boolean useAnsiJoin() {
		return delegate.useAnsiJoin();
	}
	@Override
	public boolean equals(Object obj) {
		return delegate.equals(obj);
	}
	@Override
	public int hashCode() {
		return delegate.hashCode();
	}
}
