/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.LanguageFactory;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;

/**
 * Base delegating translator. Will proxy all calls to another {@link ExecutionFactory}.
 * You will create a custom translator as a subclass of this class containing overrides for
 * any method you wish to intercept.
 * Given that subclass is given a {@link Translator} name of 'custom-delegator', your 'vdb.xml' file will
 * contain an XML fragment like the following to assign the delegate:
 * <pre>
 * {@code
 <translator type="custom-delegator" name="my-translator" description="custom translator">
    <property value="delegateName" name="name of the delegate instance"/>
    <!-- any custom properties will also appear here -->
 </translator>}
 * </pre>
 *
 */
@Translator(name="delegator", description="Translator that delegates to another translator "
        + "with capability override certain methods/capabilities")
public class BaseDelegatingExecutionFactory<F, C> extends ExecutionFactory<F, C>
        implements DelegatingExecutionFactory<F, C> {

    private String delegateName;
    private ExecutionFactory<F, C> delegate;

    /**
     * For testing only
     */
    protected ExecutionFactory<F, C> getDelegate() {
        return this.delegate;
    }

    /**
     * Sets the delegate, will be called by Teiid after {@link #start()}
     */
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
    public boolean areLobsUsableAfterClose() {
        return delegate.areLobsUsableAfterClose();
    }
    @Override
    public void closeConnection(C connection, F factory) {
        delegate.closeConnection(connection, factory);
    }
    @Override
    public ProcedureExecution createProcedureExecution(Call command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            C connection) throws TranslatorException {
        return (ProcedureExecution) delegate.createExecution(command, executionContext,
                metadata, connection);
    }
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            C connection) throws TranslatorException {
        return (ResultSetExecution) delegate.createExecution(command, executionContext,
                metadata, connection);
    }
    @Override
    public UpdateExecution createUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            C connection) throws TranslatorException {
        return (UpdateExecution) delegate.createExecution(command, executionContext,
                metadata, connection);
    }
    @Override
    public C getConnection(F factory, ExecutionContext executionContext) throws TranslatorException {
        return delegate.getConnection(factory, executionContext);
    }

    NullOrder defaultNullOrder;
    @TranslatorProperty(display="Default Null Order", advanced=true)
    @Override
    public NullOrder getDefaultNullOrder() {
        if (defaultNullOrder != null) {
            return defaultNullOrder;
        }
        return delegate.getDefaultNullOrder();
    }
    public void setDefaultNullOrder(NullOrder nullOrder) {
        defaultNullOrder = nullOrder;
    }

    @Override
    public LanguageFactory getLanguageFactory() {
        return delegate.getLanguageFactory();
    }

    Integer maxFromGroups;
    @TranslatorProperty(display = "Max FROM Allowed", description = "The number of groups supported in the from clause", advanced = true)
    @Override
    public int getMaxFromGroups() {
        if (maxFromGroups != null) {
            return maxFromGroups;
        }
        return delegate.getMaxFromGroups();
    }
    public void setMaxFromGroups(int value) {
        maxFromGroups = value;
    }

    Integer maxProjectedColumns;
    @TranslatorProperty(display = "Max projected columns allowed", description = "The number of columns supported in projected select clause", advanced = true)
    @Override
    public int getMaxProjectedColumns() {
        if (maxProjectedColumns != null) {
            return maxProjectedColumns;
        }
        return delegate.getMaxProjectedColumns();
    }
    public void setMaxProjectedColumns(int value) {
        maxProjectedColumns = value;
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

    ArrayList<String> supportedFunctions;
    @Override
    public List<String> getSupportedFunctions() {
        if (supportedFunctions == null &&
            (addSupportedFunctions != null || removeSupportedFunctions != null)) {
            supportedFunctions = new ArrayList<String>();
            List<String> baseSupportedFunctions = this.delegate.getSupportedFunctions();
            if (baseSupportedFunctions != null) {
                supportedFunctions.addAll(baseSupportedFunctions);
            }
            if (addSupportedFunctions != null) {
                supportedFunctions.addAll(Arrays.asList(addSupportedFunctions.split(","))); //$NON-NLS-1$
            }
            if (removeSupportedFunctions != null) {
                supportedFunctions.removeAll(Arrays.asList(removeSupportedFunctions.split(","))); //$NON-NLS-1$
            }
        }
        if (supportedFunctions != null) {
            return supportedFunctions;
        }
        return delegate.getSupportedFunctions();
    }

    String addSupportedFunctions;
    @TranslatorProperty(display = "Add Supported Functions(CSV)", description = "Add comma seperated names to system functions", advanced = true)
    public String getAddSupportedFunctions() {
        return addSupportedFunctions;
    }

    public void setAddSupportedFunctions(String functionNames) {
        this.addSupportedFunctions = functionNames;
    }

    String removeSupportedFunctions;
    @TranslatorProperty(display = "Remove Supported Functions(CSV)", description = "Remove comma seperated names from system functions", advanced = true)
    public String getRemoveSupportedFunctions() {
        return removeSupportedFunctions;
    }

    public void setRemoveSupportedFunctions(String functionNames) {
        this.removeSupportedFunctions = functionNames;
    }

    @Override
    public TypeFacility getTypeFacility() {
        return delegate.getTypeFacility();
    }

    @Override
    public boolean isImmutable() {
        return delegate.isImmutable();
    }
    public void setImmutable(boolean value) {
        delegate.setImmutable(value);
    }

    @TranslatorProperty(display="Is Source Required", advanced=true)
    @Override
    public boolean isSourceRequired() {
        return delegate.isSourceRequired();
    }
    public void setSourceRequired(boolean value) {
        delegate.setSourceRequired(value);
    }

    Boolean supportsAggregatesAvg;
    @TranslatorProperty(display="Supports AVG()", advanced=true)
    @Override
    public boolean supportsAggregatesAvg() {
        if (supportsAggregatesAvg != null) {
            return supportsAggregatesAvg;
        }
        return delegate.supportsAggregatesAvg();
    }
    public void setSupportsAggregatesAvg(boolean value) {
        this.supportsAggregatesAvg = value;
    }

    Boolean supportsAggregatesCount;
    @TranslatorProperty(display="Supports COUNT()", advanced=true)
    @Override
    public boolean supportsAggregatesCount() {
        if (supportsAggregatesCount != null) {
            return supportsAggregatesCount;
        }
        return delegate.supportsAggregatesCount();
    }
    public void setSupportsAggregatesCount(boolean value) {
        this.supportsAggregatesCount = value;
    }

    Boolean supportsAggregatesCountStar;
    @TranslatorProperty(display="Supports Count(*)", advanced=true)
    @Override
    public boolean supportsAggregatesCountStar() {
        if (supportsAggregatesCountStar != null) {
            return supportsAggregatesCountStar;
        }
        return delegate.supportsAggregatesCountStar();
    }
    public void setSupportsAggregatesCountStar(boolean value) {
        this.supportsAggregatesCountStar = value;
    }

    Boolean supportsAggregatesDistinct;
    @TranslatorProperty(display="Supports DISTINCT", advanced=true)
    @Override
    public boolean supportsAggregatesDistinct() {
        if (supportsAggregatesDistinct != null) {
            return supportsAggregatesDistinct;
        }
        return delegate.supportsAggregatesDistinct();
    }
    public void setSupportsAggregatesDistinct(boolean value) {
        supportsAggregatesDistinct = value;
    }

    Boolean supportsAggregatesEnhancedNumeric;
    @TranslatorProperty(display="Supports Aggegate Enhanced Numeric", advanced=true)
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        if (supportsAggregatesEnhancedNumeric != null) {
            return supportsAggregatesEnhancedNumeric;
        }
        return delegate.supportsAggregatesEnhancedNumeric();
    }
    public void setSupportsAggregatesEnhancedNumeric(boolean value) {
        supportsAggregatesEnhancedNumeric = value;
    }

    Boolean supportsAggregatesMax;
    @TranslatorProperty(display="Supports MAX", advanced=true)
    @Override
    public boolean supportsAggregatesMax() {
        if (supportsAggregatesMax != null) {
            return supportsAggregatesMax;
        }
        return delegate.supportsAggregatesMax();
    }
    public void setSupportsAggregatesMax(boolean value) {
        supportsAggregatesMax = value;
    }

    Boolean supportsAggregatesMin;
    @TranslatorProperty(display="Supports MIN", advanced=true)
    @Override
    public boolean supportsAggregatesMin() {
        if (supportsAggregatesMin != null) {
            return supportsAggregatesMin;
        }
        return delegate.supportsAggregatesMin();
    }
    public void setSupportsAggregatesMin(boolean value) {
        supportsAggregatesMin = value;
    }

    Boolean supportsAggregatesSum;
    @TranslatorProperty(display="Supports SUM", advanced=true)
    @Override
    public boolean supportsAggregatesSum() {
        if (supportsAggregatesSum != null) {
            return supportsAggregatesSum;
        }
        return delegate.supportsAggregatesSum();
    }
    public void setSupportsAggregatesSum(boolean value) {
        supportsAggregatesSum = value;
    }

    Boolean supportsAliasedTable;
    @TranslatorProperty(display="Supports Alias (ex: as A)", advanced=true)
    @Override
    public boolean supportsAliasedTable() {
        if (supportsAliasedTable != null) {
            return supportsAliasedTable;
        }
        return delegate.supportsAliasedTable();
    }
    public void setSupportsAliasedTable(boolean value) {
        supportsAliasedTable = value;
    }

    Boolean supportsBatchedUpdates;
    @TranslatorProperty(display="Supports Batched Updates", advanced=true)
    @Override
    public boolean supportsBatchedUpdates() {
        if (supportsBatchedUpdates != null) {
            return supportsBatchedUpdates;
        }
        return delegate.supportsBatchedUpdates();
    }
    public void setSupportsBatchedUpdates(boolean value) {
        supportsBatchedUpdates = value;
    }

    Boolean supportsBulkUpdate;
    @TranslatorProperty(display="Supports Bulk Updates", advanced=true)
    @Override
    public boolean supportsBulkUpdate() {
        if (supportsBulkUpdate != null) {
            return supportsBulkUpdate;
        }
        return delegate.supportsBulkUpdate();
    }
    public void setSupportsBulkUpdate(boolean value) {
        supportsBulkUpdate = value;
    }

    Boolean supportsCommonTableExpressions;
    @TranslatorProperty(display="Supports Common Table Expressions", advanced=true)
    @Override
    public boolean supportsCommonTableExpressions() {
        if (supportsCommonTableExpressions != null) {
            return supportsCommonTableExpressions;
        }
        return delegate.supportsCommonTableExpressions();
    }
    public void setSupportsCommonTableExpressions(boolean value) {
        supportsCommonTableExpressions = value;
    }

    Boolean supportsCompareCriteriaEquals;
    @TranslatorProperty(display="Supports Compare Criteria Equals", advanced=true)
    @Override
    public boolean supportsCompareCriteriaEquals() {
        if (supportsCompareCriteriaEquals != null) {
            return supportsCompareCriteriaEquals;
        }
        return delegate.supportsCompareCriteriaEquals();
    }
    public void setSupportsCompareCriteriaEquals(boolean value) {
        supportsCompareCriteriaEquals = value;
    }

    Boolean supportsCompareCriteriaOrdered;
    @TranslatorProperty(display="Supports Compare Criteria Ordered", advanced=true)
    @Override
    public boolean supportsCompareCriteriaOrdered() {
        if (supportsCompareCriteriaOrdered != null) {
            return supportsCompareCriteriaOrdered;
        }
        return delegate.supportsCompareCriteriaOrdered();
    }
    public void setSupportsCompareCriteriaOrdered(boolean value) {
        supportsCompareCriteriaOrdered = value;
    }

    Boolean supportsCorrelatedSubqueries;
    @TranslatorProperty(display="Supports Correlated Subqueries", advanced=true)
    @Override
    public boolean supportsCorrelatedSubqueries() {
        if (supportsCorrelatedSubqueries != null) {
            return supportsCorrelatedSubqueries;
        }
        return delegate.supportsCorrelatedSubqueries();
    }
    public void setSupportsCorrelatedSubqueries(boolean value) {
        supportsCorrelatedSubqueries = value;
    }

    Boolean supportsExcept;
    @TranslatorProperty(display="Supports EXCEPT", advanced=true)
    @Override
    public boolean supportsExcept() {
        if (supportsExcept != null) {
            return supportsExcept;
        }
        return delegate.supportsExcept();
    }
    public void setSupportsExcept(boolean value) {
        supportsExcept = value;
    }

    Boolean supportsExistsCriteria;
    @TranslatorProperty(display="Supports EXISTS", advanced=true)
    @Override
    public boolean supportsExistsCriteria() {
        if (supportsExistsCriteria != null) {
            return supportsExistsCriteria;
        }
        return delegate.supportsExistsCriteria();
    }
    public void setSupportsExistsCriteria(boolean value) {
        supportsExistsCriteria = value;
    }

    Boolean supportsFunctionsInGroupBy;
    @TranslatorProperty(display="Supports Functions in GROUP BY", advanced=true)
    @Override
    public boolean supportsFunctionsInGroupBy() {
        if (supportsFunctionsInGroupBy != null) {
            return supportsFunctionsInGroupBy;
        }
        return delegate.supportsFunctionsInGroupBy();
    }
    public void setSupportsFunctionsInGroupBy(boolean value) {
        supportsFunctionsInGroupBy = value;
    }

    Boolean supportsGroupBy;
    @TranslatorProperty(display="Supports GROUP BY", advanced=true)
    @Override
    public boolean supportsGroupBy() {
        if (supportsGroupBy != null) {
            return supportsGroupBy;
        }
        return delegate.supportsGroupBy();
    }
    public void setSupportsGroupBy(boolean value) {
        supportsGroupBy = value;
    }

    Boolean supportsHaving;
    @TranslatorProperty(display="Supports HAVING", advanced=true)
    @Override
    public boolean supportsHaving() {
        if (supportsHaving != null) {
            return supportsHaving;
        }
        return delegate.supportsHaving();
    }
    public void setSupportsHaving(boolean value) {
        supportsHaving = value;
    }

    Boolean supportsInCriteria;
    @TranslatorProperty(display="Supports IN", advanced=true)
    @Override
    public boolean supportsInCriteria() {
        if (supportsInCriteria != null) {
            return supportsInCriteria;
        }
        return delegate.supportsInCriteria();
    }
    public void setSupportsInCriteria(boolean value) {
        supportsInCriteria = value;
    }

    Boolean supportsInCriteriaSubquery;
    @TranslatorProperty(display="Supports IN in Subquery", advanced=true)
    @Override
    public boolean supportsInCriteriaSubquery() {
        if (supportsInCriteriaSubquery != null) {
            return supportsInCriteriaSubquery;
        }
        return delegate.supportsInCriteriaSubquery();
    }
    public void setSupportsInCriteriaSubquery(boolean value) {
        supportsInCriteriaSubquery = value;
    }

    Boolean supportsInlineViews;
    @TranslatorProperty(display="Supports Inline Views", advanced=true)
    @Override
    public boolean supportsInlineViews() {
        if (supportsInlineViews != null) {
            return supportsInlineViews;
        }
        return delegate.supportsInlineViews();
    }
    public void setSupportsInlineViews(boolean value) {
        supportsInlineViews = value;
    }

    Boolean supportsInsertWithQueryExpression;
    @TranslatorProperty(display="Supports INSERT with Query Expression", advanced=true)
    @Override
    public boolean supportsInsertWithQueryExpression() {
        if (supportsInsertWithQueryExpression != null) {
            return supportsInsertWithQueryExpression;
        }
        return delegate.supportsInsertWithQueryExpression();
    }
    public void setSupportsInsertWithQueryExpression(boolean value) {
        supportsInsertWithQueryExpression = value;
    }

    Boolean supportsIntersect;
    @TranslatorProperty(display="Supports INTERSECT", advanced=true)
    @Override
    public boolean supportsIntersect() {
        if (supportsIntersect != null) {
            return supportsIntersect;
        }
        return delegate.supportsIntersect();
    }
    public void setSupportsIntersect(boolean value) {
        supportsIntersect = value;
    }

    Boolean supportsIsNullCriteria;
    @TranslatorProperty(display="Supports ISNULL", advanced=true)
    @Override
    public boolean supportsIsNullCriteria() {
        if (supportsIsNullCriteria != null) {
            return supportsIsNullCriteria;
        }
        return delegate.supportsIsNullCriteria();
    }
    public void setSupportsIsNullCriteria(boolean value) {
        supportsIsNullCriteria = value;
    }

    Boolean supportsLikeCriteria;
    @TranslatorProperty(display="Supports LIKE", advanced=true)
    @Override
    public boolean supportsLikeCriteria() {
        if (supportsLikeCriteria != null) {
            return supportsLikeCriteria;
        }
        return delegate.supportsLikeCriteria();
    }
    public void setSupportsLikeCriteria(boolean value) {
        supportsLikeCriteria = value;
    }

    Boolean supportsLikeCriteriaEscapeCharacter;
    @TranslatorProperty(display="Supports Escape Char in LIKE", advanced=true)
    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        if (supportsLikeCriteriaEscapeCharacter != null) {
            return supportsLikeCriteriaEscapeCharacter;
        }
        return delegate.supportsLikeCriteriaEscapeCharacter();
    }
    public void setSupportsLikeCriteriaEscapeCharacter(boolean value) {
        supportsLikeCriteriaEscapeCharacter = value;
    }

    Boolean supportsNotCriteria;
    @TranslatorProperty(display="Supports NOT", advanced=true)
    @Override
    public boolean supportsNotCriteria() {
        if (supportsNotCriteria != null) {
            return supportsNotCriteria;
        }
        return delegate.supportsNotCriteria();
    }
    public void setSupportsNotCriteria(boolean value) {
        supportsNotCriteria = value;
    }

    Boolean supportsOrCriteria;
    @TranslatorProperty(display="Supports OR", advanced=true)
    @Override
    public boolean supportsOrCriteria() {
        if (supportsOrCriteria != null) {
            return supportsOrCriteria;
        }
        return delegate.supportsOrCriteria();
    }
    public void setSupportsOrCriteria(boolean value) {
        supportsOrCriteria = value;
    }

    Boolean supportsOrderByNullOrdering;
    @TranslatorProperty(display="Supports ORDER BY col NULLS [FIRST|LAST|HIGH|LOW]", advanced=true)
    @Override
    public boolean supportsOrderByNullOrdering() {
        if (supportsOrderByNullOrdering != null) {
            return supportsOrderByNullOrdering;
        }
        return delegate.supportsOrderByNullOrdering();
    }
    public void setSupportsOrderByNullOrdering(boolean value) {
        supportsOrderByNullOrdering = value;
    }

    Boolean supportsOrderByUnrelated;
    @TranslatorProperty(display="Supports ORDER BY Unrelated", advanced=true)
    @Override
    public boolean supportsOrderByUnrelated() {
        if (supportsOrderByUnrelated != null) {
            return supportsOrderByUnrelated;
        }
        return delegate.supportsOrderByUnrelated();
    }
    public void setSupportsOrderByUnrelated(boolean value) {
        supportsOrderByUnrelated = value;
    }

    Boolean supportsQuantifiedCompareCriteriaAll;
    @TranslatorProperty(display="Supports ALL", advanced=true)
    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        if (supportsQuantifiedCompareCriteriaAll != null) {
            return supportsQuantifiedCompareCriteriaAll;
        }
        return delegate.supportsQuantifiedCompareCriteriaAll();
    }
    public void setSupportsQuantifiedCompareCriteriaAll(boolean value) {
        supportsQuantifiedCompareCriteriaAll = value;
    }

    Boolean supportsQuantifiedCompareCriteriaSome;
    @TranslatorProperty(display="Supports SOME", advanced=true)
    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        if (supportsQuantifiedCompareCriteriaSome != null) {
            return supportsQuantifiedCompareCriteriaSome;
        }
        return delegate.supportsQuantifiedCompareCriteriaSome();
    }
    public void setSupportsQuantifiedCompareCriteriaSome(boolean value) {
        supportsQuantifiedCompareCriteriaSome = value;
    }

    Boolean supportsRowLimit;
    @TranslatorProperty(display="Supports LIMIT", advanced=true)
    @Override
    public boolean supportsRowLimit() {
        if (supportsRowLimit != null) {
            return supportsRowLimit;
        }
        return delegate.supportsRowLimit();
    }
    public void setSupportsRowLimit(boolean value) {
        supportsRowLimit = value;
    }

    Boolean supportsRowOffset;
    @TranslatorProperty(display="Supports LIMIT OFFSET", advanced=true)
    @Override
    public boolean supportsRowOffset() {
        if (supportsRowOffset != null) {
            return supportsRowOffset;
        }
        return delegate.supportsRowOffset();
    }
    public void setSupportsRowOffset(boolean value) {
        supportsRowOffset = value;
    }

    Boolean supportsScalarSubqueries;
    @TranslatorProperty(display="Supports Scalar Sub-Queries", advanced=true)
    @Override
    public boolean supportsScalarSubqueries() {
        if (supportsScalarSubqueries != null) {
            return supportsScalarSubqueries;
        }
        return delegate.supportsScalarSubqueries();
    }
    public void setSupportsScalarSubqueries(boolean value) {
        supportsScalarSubqueries = value;
    }

    Boolean supportsSearchedCaseExpressions;
    @TranslatorProperty(display="Supports CASE Expression", advanced=true)
    @Override
    public boolean supportsSearchedCaseExpressions() {
        if (supportsSearchedCaseExpressions != null) {
            return supportsSearchedCaseExpressions;
        }
        return delegate.supportsSearchedCaseExpressions();
    }
    public void setSupportsSearchedCaseExpressions(boolean value) {
        supportsSearchedCaseExpressions = value;
    }

    Boolean supportsSelectExpression;
    @TranslatorProperty(display="Supports SELECT expressions", advanced=true)
    @Override
    public boolean supportsSelectExpression() {
        if (supportsSelectExpression != null) {
            return supportsSelectExpression;
        }
        return delegate.supportsSelectExpression();
    }
    public void setSupportsSelectExpression(boolean value) {
        supportsSelectExpression = value;
    }

    Boolean supportsSelfJoins;
    @TranslatorProperty(display="Supports Self JOINS", advanced=true)
    @Override
    public boolean supportsSelfJoins() {
        if (supportsSelfJoins != null) {
            return supportsSelfJoins;
        }
        return delegate.supportsSelfJoins();
    }
    public void setSupportsSelfJoins(boolean value) {
        supportsSelfJoins = value;
    }

    Boolean supportsSetQueryOrderBy;
    @TranslatorProperty(display="Supports Set Query Orderby", advanced=true)
    @Override
    public boolean supportsSetQueryOrderBy() {
        if (supportsSetQueryOrderBy != null) {
            return supportsSetQueryOrderBy;
        }
        return delegate.supportsSetQueryOrderBy();
    }
    public void setSupportsSetQueryOrderBy(boolean value) {
        supportsSetQueryOrderBy = value;
    }

    Boolean supportsUnions;
    @TranslatorProperty(display="Supports UNION", advanced=true)
    @Override
    public boolean supportsUnions() {
        if (supportsUnions != null) {
            return supportsUnions;
        }
        return delegate.supportsUnions();
    }
    public void setSupportsUnions(boolean value) {
        supportsUnions = value;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    Boolean useAnsiJoin;
    @TranslatorProperty(display="Supports ANSI Joins", advanced=true)
    @Override
    public boolean useAnsiJoin() {
        if (useAnsiJoin != null) {
            return useAnsiJoin;
        }
        return delegate.useAnsiJoin();
    }
    public void setUseAnsiJoin(boolean value) {
        useAnsiJoin = value;
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }
    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @TranslatorProperty(display="Supports Copy LOBS", advanced=true)
    @Override
    public boolean isCopyLobs() {
        return delegate.isCopyLobs();
    }
    public void setCopyLobs(boolean value) {
        delegate.setCopyLobs(value);
    }

    Boolean supportsArrayAgg;
    @TranslatorProperty(display="Supports Array Aggregation", advanced=true)
    @Override
    public boolean supportsArrayAgg() {
        if (supportsArrayAgg != null) {
            return supportsArrayAgg;
        }
        return delegate.supportsArrayAgg();
    }
    public void setSupportsArrayAgg(boolean value) {
        supportsArrayAgg = value;
    }

    Boolean supportsElementaryOlapOperations;
    @TranslatorProperty(display="Supports OLAP Operations", advanced=true)
    @Override
    public boolean supportsElementaryOlapOperations() {
        if (supportsElementaryOlapOperations != null) {
            return supportsElementaryOlapOperations;
        }
        return delegate.supportsElementaryOlapOperations();
    }
    public void setSupportsElementaryOlapOperations(boolean value) {
        supportsElementaryOlapOperations = value;
    }

    Boolean supportsWindowFrameClause;
    @TranslatorProperty(display="Supports Window Frame Clause on a Window Function", advanced=true)
    @Override
    public boolean supportsWindowFrameClause() {
        if (supportsWindowFrameClause != null) {
            return supportsWindowFrameClause;
        }
        return delegate.supportsWindowFrameClause();
    }
    public void setSupportsWindowFrameClause(boolean value) {
        supportsWindowFrameClause = value;
    }

    @Override
    public boolean supportsFormatLiteral(String literal,
            org.teiid.translator.ExecutionFactory.Format format) {
        return delegate.supportsFormatLiteral(literal, format);
    }

    Boolean supportsLikeRegex;
    @TranslatorProperty(display="Supports REGEX in LIKE", advanced=true)
    @Override
    public boolean supportsLikeRegex() {
        if (supportsLikeRegex != null) {
            return supportsLikeRegex;
        }
        return delegate.supportsLikeRegex();
    }
    public void setSupportsLikeRegex(boolean value) {
        supportsLikeRegex = value;
    }

    Boolean supportsOnlyFormatLiterals;
    @TranslatorProperty(display="Supports only Format Literals", advanced=true)
    @Override
    public boolean supportsOnlyFormatLiterals() {
        if (supportsOnlyFormatLiterals != null) {
            return supportsOnlyFormatLiterals;
        }
        return delegate.supportsOnlyFormatLiterals();
    }
    public void setSupportsOnlyFormatLiterals(boolean value) {
        supportsOnlyFormatLiterals = value;
    }

    Boolean supportsOnlySingleTableGroupBy;
    @TranslatorProperty(display="Supports Single Table GROUP BY", advanced=true)
    @Override
    public boolean supportsOnlySingleTableGroupBy() {
        if (supportsOnlySingleTableGroupBy != null) {
            return supportsOnlySingleTableGroupBy;
        }
        return delegate.supportsOnlySingleTableGroupBy();
    }
    public void setSupportsOnlySingleTableGroupBy(boolean value) {
        supportsOnlySingleTableGroupBy = value;
    }

    Boolean supportsSimilarTo;
    @TranslatorProperty(display="Supports SIMILAR TO", advanced=true)
    @Override
    public boolean supportsSimilarTo() {
        if (supportsSimilarTo != null) {
            return supportsSimilarTo;
        }
        return delegate.supportsSimilarTo();
    }
    public void setSupportsSimilarTo(boolean value) {
        supportsSimilarTo = value;
    }

    Boolean supportsWindowDistinctAggregates;
    @TranslatorProperty(display="Supports Windowed Aggregates", advanced=true)
    @Override
    public boolean supportsWindowDistinctAggregates() {
        if (supportsWindowDistinctAggregates != null) {
            return supportsWindowDistinctAggregates;
        }
        return delegate.supportsWindowDistinctAggregates();
    }
    public void setSupportsWindowDistinctAggregates(boolean value) {
        supportsWindowDistinctAggregates = value;
    }

    Boolean supportsWindowOrderByWithAggregates;
    @TranslatorProperty(display="Supports Order by With Windowed Aggregates", advanced=true)
    @Override
    public boolean supportsWindowOrderByWithAggregates() {
        if (supportsWindowOrderByWithAggregates != null) {
            return supportsWindowOrderByWithAggregates;
        }
        return delegate.supportsWindowOrderByWithAggregates();
    }
    public void setSupportsWindowOrderByWithAggregates(boolean value) {
        supportsWindowOrderByWithAggregates = value;
    }

    @Override
    public int getMaxInCriteriaSize() {
        return delegate.getMaxInCriteriaSize();
    }
    public void setMaxInCriteriaSize(int max) {
        delegate.setMaxInCriteriaSize(max);
    }

    @Override
    public org.teiid.translator.ExecutionFactory.SupportedJoinCriteria getSupportedJoinCriteria() {
        return delegate.getSupportedJoinCriteria();
    }
    public void setSupportedJoinCriteria(org.teiid.translator.ExecutionFactory.SupportedJoinCriteria value) {
        this.delegate.setSupportedJoinCriteria(value);
    }

    @Override
    public boolean requiresCriteria() {
        return delegate.requiresCriteria();
    }
    public void setRequiresCriteria(boolean value) {
        delegate.setRequiresCriteria(value);
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return delegate.supportsFullOuterJoins();
    }
    @Override
    public void setSupportsFullOuterJoins(boolean supportsFullOuterJoins) {
        delegate.setSupportsFullOuterJoins(supportsFullOuterJoins);
    }

    @Override
    public boolean supportsInnerJoins() {
        return delegate.supportsInnerJoins();
    }
    @Override
    public void setSupportsInnerJoins(boolean supportsInnerJoins) {
        delegate.setSupportsInnerJoins(supportsInnerJoins);
    }

    @Override
    public boolean supportsOrderBy() {
        return delegate.supportsOrderBy();
    }

    @Override
    public void setSupportsOrderBy(boolean supportsOrderBy) {
        delegate.setSupportsOrderBy(supportsOrderBy);
    }

    @Override
    public boolean supportsOuterJoins() {
        return delegate.supportsOuterJoins();
    }
    @Override
    public void setSupportsOuterJoins(boolean supportsOuterJoins) {
        delegate.setSupportsOuterJoins(supportsOuterJoins);
    }

    @Override
    public boolean supportsSelectDistinct() {
        return delegate.supportsSelectDistinct();
    }
    @Override
    public void setSupportsSelectDistinct(boolean supportsSelectDistinct) {
        delegate.setSupportsSelectDistinct(supportsSelectDistinct);
    }

    @Override
    public int getMaxDependentInPredicates() {
        return delegate.getMaxDependentInPredicates();
    }
    @Override
    public void setMaxDependentInPredicates(int maxDependentInPredicates) {
        this.delegate.setMaxDependentInPredicates(maxDependentInPredicates);
    }

    Boolean supportsAdvancedOlapOperations;
    @TranslatorProperty(display="Supports Advanced OLAP Operations", advanced=true)
    @Override
    public boolean supportsAdvancedOlapOperations() {
        if (supportsAdvancedOlapOperations != null) {
            return supportsAdvancedOlapOperations;
        }
        return delegate.supportsAdvancedOlapOperations();
    }
    public void setSupportsAdvancedOlapOperations(boolean value) {
        supportsAdvancedOlapOperations = value;
    }

    Boolean supportsSubqueryInOn;
    @TranslatorProperty(display="Supports Subquery In ON", advanced=true)
    @Override
    public boolean supportsSubqueryInOn() {
        if (supportsSubqueryInOn != null) {
            return supportsSubqueryInOn;
        }
        return delegate.supportsSubqueryInOn();
    }
    public void setSupportsSubqueryInOn(boolean value) {
        supportsSubqueryInOn = value;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return delegate.supportsConvert(fromType, toType);
    }

    Boolean supportsDependentJoins;
    @TranslatorProperty(display="Supports Dependent Joins", advanced=true)
    @Override
    public boolean supportsDependentJoins() {
        if (supportsDependentJoins != null) {
            return supportsDependentJoins;
        }
        return delegate.supportsDependentJoins();
    }
    public void setSupportsDependentJoins(boolean value) {
        supportsDependentJoins = value;
    }

    Boolean supportsOnlyLiteralComparison;
    @TranslatorProperty(display="Supports Only Literal Comparision", advanced=true)
    @Override
    public boolean supportsOnlyLiteralComparison() {
        if (supportsOnlyLiteralComparison != null) {
            return supportsOnlyLiteralComparison;
        }
        return delegate.supportsOnlyLiteralComparison();
    }
    public void setSupportsOnlyLiteralComparison(boolean value) {
        supportsOnlyLiteralComparison = value;
    }

    @Override
    public CacheDirective getCacheDirective(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata)
            throws TranslatorException {
        if (cachePattern != null && cachePattern.matcher(command.toString()).matches()) {
            //return a new cache directive with defaults and ttl set
            CacheDirective cacheDirective = new CacheDirective();
            cacheDirective.setTtl(cacheTtl);
            return cacheDirective;
        }
        return delegate.getCacheDirective(command, executionContext, metadata);
    }

    @TranslatorProperty(display="Is Source Required For Metadata", advanced=true)
    @Override
    public boolean isSourceRequiredForMetadata() {
        return delegate.isSourceRequiredForMetadata();
    }
    public void setSourceRequiredForMetadata(boolean value) {
        delegate.setSourceRequiredForMetadata(value);
    }

    Boolean forkable;
    @TranslatorProperty(display="Is Forkable", description="When forkable the engine may use a separate thread to interact with returned Exection", advanced=true)
    @Override
    public boolean isForkable() {
        if (forkable != null) {
            return forkable;
        }
        return delegate.isForkable();
    }
    public void setForkable(boolean value) {
        forkable = value;
    }

    Boolean supportsArrayType;
    @TranslatorProperty(display="Supports Array Type", advanced=true)
    @Override
    public boolean supportsArrayType() {
        if (supportsArrayType != null) {
            return supportsArrayType;
        }
        return delegate.supportsArrayType();
    }
    public void setSupportsArrayType(boolean value) {
        supportsArrayType = value;
    }

    @Override
    @TranslatorProperty(display="Direct Query Procedure Name", advanced=true)
    public String getDirectQueryProcedureName() {
        return delegate.getDirectQueryProcedureName();
    }
    public void setDirectQueryProcedureName(String name) {
        this.delegate.setDirectQueryProcedureName(name);
    }

    @Override
    public boolean supportsDirectQueryProcedure() {
        return delegate.supportsDirectQueryProcedure();
    }
    public void setSupportsDirectQueryProcedure(boolean value) {
        delegate.setSupportsDirectQueryProcedure(value);
    }

    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments,
            Command command, ExecutionContext executionContext,
            RuntimeMetadata metadata, C connection) throws TranslatorException {
         return delegate.createDirectExecution(arguments, command, executionContext, metadata, connection);
    }

    Boolean supportsOnlyCorrelatedSubqueries;
    @TranslatorProperty(display="Supports Correlated Sub Queries", advanced=true)
    @Override
    public boolean supportsOnlyCorrelatedSubqueries() {
        if (supportsOnlyCorrelatedSubqueries != null) {
            return supportsOnlyCorrelatedSubqueries;
        }
        return delegate.supportsOnlyCorrelatedSubqueries();
    }
    public void setSupportsOnlyCorrelatedSubqueries(boolean value) {
        supportsOnlyCorrelatedSubqueries = value;
    }

    Boolean sourceRequiredForCapabilities;
    @TranslatorProperty(display="Source required for Capabilities", advanced=true)
    @Override
    public boolean isSourceRequiredForCapabilities() {
        if (sourceRequiredForCapabilities != null) {
            return sourceRequiredForCapabilities;
        }
        return delegate.isSourceRequiredForCapabilities();
    }
    public void setSourceRequiredForCapabilities(boolean value) {
        sourceRequiredForCapabilities = value;
    }

    @Override
    public void initCapabilities(C connection) throws TranslatorException {
        delegate.initCapabilities(connection);
    }

    Boolean supportsStringAgg;
    @TranslatorProperty(display="Supports STRING_AGG", advanced=true)
    @Override
    public boolean supportsStringAgg() {
        if (supportsStringAgg != null) {
            return supportsStringAgg;
        }
        return delegate.supportsStringAgg();
    }
    public void setSupportsStringAgg(boolean value) {
        supportsStringAgg = value;
    }

    Boolean supportsListAgg;
    @TranslatorProperty(display="Supports LISTAGG", advanced=true)
    @Override
    public boolean supportsListAgg() {
        if (supportsListAgg != null) {
            return supportsListAgg;
        }
        return delegate.supportsListAgg();
    }
    public void setSupportsListAgg(boolean value) {
        supportsListAgg = value;
    }

    Boolean supportsFullDependentJoins;
    @TranslatorProperty(display="Supports Full Dependent Joins", advanced=true)
    @Override
    public boolean supportsFullDependentJoins() {
        if (supportsFullDependentJoins != null) {
            return supportsFullDependentJoins;
        }
        return delegate.supportsFullDependentJoins();
    }
    public void setSupportsFullDependentJoins(boolean value) {
        supportsFullDependentJoins = value;
    }

    Boolean supportsSelectWithoutFrom;
    @TranslatorProperty(display="Supports SELECT w/o FROM", advanced=true)
    @Override
    public boolean supportsSelectWithoutFrom() {
        if (supportsSelectWithoutFrom != null) {
            return supportsSelectWithoutFrom;
        }
        return delegate.supportsSelectWithoutFrom();
    }
    public void setSupportsSelectWithoutFrom(boolean value) {
        supportsSelectWithoutFrom = value;
    }

    Boolean supportsGroupByRollup;
    @TranslatorProperty(display="Supports GROUP BY ROLLUP", advanced=true)
    @Override
    public boolean supportsGroupByRollup() {
        if (supportsGroupByRollup != null) {
            return supportsGroupByRollup;
        }
        return delegate.supportsGroupByRollup();
    }
    public void setSupportsGroupByRollup(boolean value) {
        supportsGroupByRollup = value;
    }

    Boolean supportsOrderByWithExtendedGrouping;
    @TranslatorProperty(display="Supports Orderby w/extended grouping", advanced=true)
    @Override
    public boolean supportsOrderByWithExtendedGrouping() {
        if (supportsOrderByWithExtendedGrouping != null) {
            return supportsOrderByWithExtendedGrouping;
        }
        return delegate.supportsOrderByWithExtendedGrouping();
    }
    public void setSupportsOrderByWithExtendedGrouping(boolean value) {
        supportsOrderByWithExtendedGrouping = value;
    }

    @Override
    public boolean isThreadBound() {
        return delegate.isThreadBound();
    }
    public void setThreadBound(boolean value) {
        delegate.setThreadBound(value);
    }

    @Override
    public String getCollationLocale() {
        return delegate.getCollationLocale();
    }
    public void setCollationLocale(String value) {
        delegate.setCollationLocale(value);
    }

    Boolean supportsRecursiveCommonTableExpressions;
    @TranslatorProperty(display="Supports Recursive Common Table Expresions", advanced=true)
    @Override
    public boolean supportsRecursiveCommonTableExpressions() {
        if (supportsRecursiveCommonTableExpressions != null) {
            return supportsRecursiveCommonTableExpressions;
        }
        return delegate.supportsRecursiveCommonTableExpressions();
    }
    public void setSupportsRecursiveCommonTableExpressions(boolean value) {
        supportsRecursiveCommonTableExpressions = value;
    }

    Boolean supportsCompareCriteriaOrderedExclusive;
    @TranslatorProperty(display="Supports Criteria Ordered Exclusive", advanced=true)
    @Override
    public boolean supportsCompareCriteriaOrderedExclusive() {
        if (supportsCompareCriteriaOrderedExclusive != null) {
            return supportsCompareCriteriaOrderedExclusive;
        }
        return delegate.supportsCompareCriteriaOrderedExclusive();
    }
    @Deprecated
    public void supportsCompareCriteriaOrderedExclusive(boolean value) {
        supportsCompareCriteriaOrderedExclusive = value;
    }
    public void setSupportsCompareCriteriaOrderedExclusive(boolean value) {
        supportsCompareCriteriaOrderedExclusive = value;
    }

    @Override
    public boolean returnsSingleUpdateCount() {
        return delegate.returnsSingleUpdateCount();
    }

    Boolean supportsPartialFiltering;
    @TranslatorProperty(display="Supports Partial Filtering", advanced=true)
    @Override
    public boolean supportsPartialFiltering() {
        if (supportsPartialFiltering != null) {
            return supportsPartialFiltering;
        }
        return delegate.supportsPartialFiltering();
    }
    public void setSupportsPartialFiltering(boolean value) {
        supportsPartialFiltering = value;
    }

    Boolean useBindingsForDependentJoin;
    @TranslatorProperty(display="Use Bindings for Dependent Joins", advanced=true)
    @Override
    public boolean useBindingsForDependentJoin() {
        if (useBindingsForDependentJoin != null) {
            return useBindingsForDependentJoin;
        }
        return delegate.useBindingsForDependentJoin();
    }
    public void setUseBindingsForDependentJoin(boolean value) {
        useBindingsForDependentJoin = value;
    }

    Boolean supportsSubqueryCommonTableExpressions;
    @TranslatorProperty(display="Supports Subquery Common Table Expressions", advanced=true)
    @Override
    public boolean supportsSubqueryCommonTableExpressions() {
        if (supportsSubqueryCommonTableExpressions != null) {
            return supportsSubqueryCommonTableExpressions;
        }
        return delegate.supportsSubqueryCommonTableExpressions();
    }
    public void setSupportsSubqueryCommonTableExpressions(boolean value) {
        supportsSubqueryCommonTableExpressions = value;
    }

    Boolean supportsCorrelatedSubqueryLimit;
    @TranslatorProperty(display="Supports Correlated Subquery Limit", advanced=true)
    @Override
    public boolean supportsCorrelatedSubqueryLimit() {
        if (supportsCorrelatedSubqueryLimit != null) {
            return supportsCorrelatedSubqueryLimit;
        }
        return delegate.supportsCorrelatedSubqueryLimit();
    }
    public void setSupportsCorrelatedSubqueryLimit(boolean value) {
        supportsCorrelatedSubqueryLimit = value;
    }

    Character requiredLikeEscape;
    @TranslatorProperty(display="Escape char for LIKE", advanced=true)
    @Override
    public Character getRequiredLikeEscape() {
        if (requiredLikeEscape != null) {
            return requiredLikeEscape;
        }
        return delegate.getRequiredLikeEscape();
    }
    public void setRequiredLikeEscape(Character c) {
        requiredLikeEscape = c;
    }

    Boolean supportsScalarSubqueryProjection;
    @TranslatorProperty(display="Supports Scalar SubQuery in SELECT", advanced=true)
    @Override
    public boolean supportsScalarSubqueryProjection() {
        if (supportsScalarSubqueryProjection != null) {
            return supportsScalarSubqueryProjection;
        }
        return delegate.supportsScalarSubqueryProjection();
    }
    public void setSupportsScalarSubqueryProjection(boolean value) {
        supportsScalarSubqueryProjection = value;
    }

    @Override
    public org.teiid.translator.ExecutionFactory.TransactionSupport getTransactionSupport() {
        return delegate.getTransactionSupport();
    }

    public void setTransactionSupport(TransactionSupport transactionSupport) {
        delegate.setTransactionSupport(transactionSupport);
    }

    @Override
    @TranslatorProperty(display="Excluded Common Table Expression Name", advanced=true)
    public String getExcludedCommonTableExpressionName() {
        return delegate.getExcludedCommonTableExpressionName();
    }
    public void setExcludedCommonTableExpressionName(String value) {
        delegate.setExcludedCommonTableExpressionName(value);
    }

    Boolean supportsLateralJoin;
    @TranslatorProperty(display="Supports Lateral Join", advanced=true)
    @Override
    public boolean supportsLateralJoin() {
        if (supportsLateralJoin != null) {
            return supportsLateralJoin;
        }
        return delegate.supportsLateralJoin();
    }
    public void setSupportsLateralJoin(boolean value) {
        supportsLateralJoin = value;
    }

    Boolean supportsLateralJoinCondition;
    @TranslatorProperty(display="Supports Lateral Join Condition", advanced=true)
    @Override
    public boolean supportsLateralJoinCondition() {
        if (supportsLateralJoinCondition != null) {
            return supportsLateralJoinCondition;
        }
        return delegate.supportsLateralJoinCondition();
    }
    public void setSupportsLateralJoinCondition(boolean value) {
        supportsLateralJoinCondition = value;
    }

    Boolean supportsProcedureTable;
    @TranslatorProperty(display="Supports Procedure Table", advanced=true)
    @Override
    public boolean supportsProcedureTable() {
        if (supportsProcedureTable != null) {
            return supportsProcedureTable;
        }
        return delegate.supportsProcedureTable();
    }
    public void setSupportsProcedureTable(boolean value) {
        supportsProcedureTable = value;
    }

    Boolean supportsGroupByMultipleDistinctAggregates;
    @TranslatorProperty(display="Supports GROUP By with Multiple DISTINCTS", advanced=true)
    @Override
    public boolean supportsGroupByMultipleDistinctAggregates() {
        if (supportsGroupByMultipleDistinctAggregates != null) {
            return supportsGroupByMultipleDistinctAggregates;
        }
        return delegate.supportsGroupByMultipleDistinctAggregates();
    }
    public void setSupportsGroupByMultipleDistinctAggregates(boolean value) {
        supportsGroupByMultipleDistinctAggregates = value;
    }

    @Override
    public void start() throws TranslatorException {
        //nothing to do
    }

    Boolean supportsUpsert;
    @TranslatorProperty(display="Supports Upsert", advanced=true)
    @Override
    public boolean supportsUpsert() {
        if (supportsUpsert != null) {
            return supportsUpsert;
        }
        return delegate.supportsUpsert();
    }
    public void setSupportsUpsert(boolean value) {
        supportsUpsert = value;
    }

    Boolean supportsSelectExpressionArrayType;
    @TranslatorProperty(display="Supports SELECT array type expressions", advanced=true)
    @Override
    public boolean supportsSelectExpressionArrayType() {
        if (supportsSelectExpressionArrayType != null) {
            return supportsSelectExpressionArrayType;
        }
        return delegate.supportsSelectExpressionArrayType();
    }
    public void setSupportsSelectExpressionArrayType(boolean value) {
        supportsSelectExpressionArrayType = value;
    }

    Boolean supportsSetQueryLimitOffset;
    @TranslatorProperty(display="Supports SET Query OFFSET/LIMIT", advanced=true)
    @Override
    public boolean supportsSetQueryLimitOffset() {
        if (supportsSetQueryLimitOffset != null) {
            return supportsSetQueryLimitOffset;
        }
        return delegate.supportsSetQueryLimitOffset();
    }
    public void setSupportsSetQueryLimitOffset(boolean value) {
        supportsSetQueryLimitOffset = value;
    }

    Boolean supportsIsDistinctCriteria;
    @TranslatorProperty(display="Supports IS DISTINCT", advanced=true)
    @Override
    public boolean supportsIsDistinctCriteria() {
        if (supportsIsDistinctCriteria != null) {
            return supportsIsDistinctCriteria;
        }
        return delegate.supportsIsDistinctCriteria();
    }
    public void setSupportsIsDistinctCriteria(boolean value) {
        supportsIsDistinctCriteria = value;
    }

    Boolean supportsOnlyLateralJoinProcedure;
    @TranslatorProperty(display="Supports Only Lateral Join Procedure", advanced=true)
    @Override
    public boolean supportsOnlyLateralJoinProcedure() {
        if (supportsOnlyLateralJoinProcedure != null) {
            return supportsOnlyLateralJoinProcedure;
        }
        return delegate.supportsOnlyLateralJoinProcedure();
    }
    public void setSupportsOnlyLateralJoinProcedure(
            boolean supportsOnlyLateralJoinProcedure) {
        this.supportsOnlyLateralJoinProcedure = supportsOnlyLateralJoinProcedure;
    }

    Boolean supportsOnlyTimestampAddLiteral;
    @TranslatorProperty(display="Supports Only TimestampAdd literal", advanced=true)
    @Override
    public boolean supportsOnlyTimestampAddLiteral() {
        if (supportsOnlyTimestampAddLiteral != null) {
            return supportsOnlyTimestampAddLiteral;
        }
        return delegate.supportsOnlyTimestampAddLiteral();
    }
    public void setSupportsOnlyTimestampAddLiteral(
            boolean supportsOnlyTimestampAddLiteral) {
        this.supportsOnlyTimestampAddLiteral = supportsOnlyTimestampAddLiteral;
    }

    private Pattern cachePattern;
    @TranslatorProperty(display="Cache Pattern", advanced=true)
    public String getCachePattern() {
        if (this.cachePattern != null) {
            return this.cachePattern.pattern();
        }
        return null;
    }

    public void setCachePattern(String cachePattern) {
        this.cachePattern = Pattern.compile(cachePattern);
    }

    Long cacheTtl;
    @TranslatorProperty(display="Cache TTL", advanced=true)
    public Long getCacheTtl() {
        return cacheTtl;
    }

    public void setCacheTtl(Long ttl) {
        this.cacheTtl = ttl;
    }

    Boolean supportsNtile;
    @TranslatorProperty(display="Supports NTILE", advanced=true)
    @Override
    public boolean supportsWindowFunctionNtile() {
        if (supportsNtile != null) {
            return supportsNtile;
        }
        return delegate.supportsWindowFunctionNtile();
    }

    public void setSupportsWindowFunctionNtile(boolean supportsNtile) {
        this.supportsNtile = supportsNtile;
    }

    Boolean supportsPercentRank;
    @TranslatorProperty(display="Supports PERCENT_RANK", advanced=true)
    @Override
    public boolean supportsWindowFunctionPercentRank() {
        if (supportsPercentRank != null) {
            return supportsPercentRank;
        }
        return delegate.supportsWindowFunctionPercentRank();
    }

    public void setSupportsWindowFunctionPercentRank(boolean supportsPercentRank) {
        this.supportsPercentRank = supportsPercentRank;
    }

    Boolean supportsCumeDist;
    @TranslatorProperty(display="Supports CUME_DIST", advanced=true)
    @Override
    public boolean supportsWindowFunctionCumeDist() {
        if (supportsCumeDist != null) {
            return supportsCumeDist;
        }
        return delegate.supportsWindowFunctionCumeDist();
    }

    public void setSupportsWindowFunctionCumeDist(boolean supportsCumeDist) {
        this.supportsCumeDist = supportsCumeDist;
    }

    Boolean supportsNthValue;
    @TranslatorProperty(display="Supports NTH_VALUE", advanced=true)
    @Override
    public boolean supportsWindowFunctionNthValue() {
        if (supportsNthValue != null) {
            return supportsNthValue;
        }
        return delegate.supportsWindowFunctionNthValue();
    }

    public void setSupportsWindowFunctionNthValue(boolean supportsNthValue) {
        this.supportsNthValue = supportsNthValue;
    }

    Boolean supportsMultipleOpenStatements;
    @Override
    public boolean supportsMultipleOpenExecutions() {
        if (supportsMultipleOpenStatements != null) {
            return supportsMultipleOpenStatements;
        }
        return delegate.supportsMultipleOpenExecutions();
    }

    public void setSupportsMultipleOpenStatements(
            boolean supportsMultipleOpenStatements) {
        this.supportsMultipleOpenStatements = supportsMultipleOpenStatements;
    }

    Boolean supportsAggregatesCountBig;
    @Override
    public boolean supportsAggregatesCountBig() {
        if (supportsAggregatesCountBig != null) {
            return supportsAggregatesCountBig;
        }
        return delegate.supportsAggregatesCountBig();
    }

    public void setSupportsAggregatesCountBig(
            boolean supportsAggregatesCountBig) {
        this.supportsAggregatesCountBig = supportsAggregatesCountBig;
    }

    Boolean supportsGeographyType;
    @Override
    public boolean supportsGeographyType() {
        if (supportsGeographyType != null) {
            return supportsGeographyType;
        }
        return delegate.supportsGeographyType();
    }

    public void setSupportsGeographyType(
            boolean supportsGeographyType) {
        this.supportsGeographyType = supportsGeographyType;
    }

    Boolean supportsProcedureParameterExpression;
    @Override
    public boolean supportsProcedureParameterExpression() {
        if (supportsProcedureParameterExpression != null) {
            return supportsProcedureParameterExpression;
        }
        return delegate.supportsGeographyType();
    }

    public void setSupportsProcedureParameterExpression(
            Boolean supportsProcedureParameterExpression) {
        this.supportsProcedureParameterExpression = supportsProcedureParameterExpression;
    }

    Boolean supportsOnlyRelationshipStyleJoins;
    @Override
    public boolean supportsOnlyRelationshipStyleJoins() {
        if (supportsOnlyRelationshipStyleJoins != null) {
            return supportsOnlyRelationshipStyleJoins;
        }
        return delegate.supportsOnlyRelationshipStyleJoins();
    }

    public void setSupportsOnlyRelationshipStyleJoins(
            Boolean supportsOnlyRelationshipStyleJoins) {
        this.supportsOnlyRelationshipStyleJoins = supportsOnlyRelationshipStyleJoins;
    }
}
