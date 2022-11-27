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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.teiid.connector.DataPlugin;
import org.teiid.core.TeiidException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.Argument;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.LanguageFactory;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.api.Connection;
import org.teiid.resource.api.ConnectionFactory;
import org.teiid.translator.CacheDirective.Scope;
import org.teiid.translator.TypeFacility.RUNTIME_CODES;
import org.teiid.translator.TypeFacility.RUNTIME_NAMES;



/**
 * <p>The primary entry point for a Translator.  This class should be extended by the custom translator writer.
 *
 * The deployer instantiates this class through reflection. So it is important to have no-arg constructor. Once constructed
 * the "start" method is called. This class represents the basic capabilities of the translator.
 */
public class ExecutionFactory<F, C> {

    public enum TransactionSupport {
        XA,
        LOCAL,
        NONE
    }

    public enum SupportedJoinCriteria {
        /**
         * Indicates that any supported criteria is allowed.
         */
        ANY,
        /**
         * Indicates that any simple comparison of elements is allowed.
         */
        THETA,
        /**
         * Indicates that only equality predicates of elements are allowed.
         */
        EQUI,
        /**
         * Indicates that only equality predicates between
         * exactly one primary and foreign key is allowed per join.
         */
        KEY
    }

    public enum NullOrder {
        HIGH,
        LOW,
        FIRST,
        LAST,
        UNKNOWN
    }

    public static final int DEFAULT_MAX_FROM_GROUPS = -1;
    public static final int DEFAULT_MAX_PROJECTED_COLUMNS = -1;
    public static final int DEFAULT_MAX_IN_CRITERIA_SIZE = -1;

    private static final TypeFacility TYPE_FACILITY = new TypeFacility();

    /*
     * Managed execution properties
     */
    private boolean immutable;
    private boolean sourceRequired = true;
    private Boolean sourceRequiredForMetadata;
    private boolean threadBound;

    /*
     * Support properties
     */
    private boolean supportsSelectDistinct;
    private boolean supportsOuterJoins;
    private SupportedJoinCriteria supportedJoinCriteria = SupportedJoinCriteria.ANY;
    private boolean supportsOrderBy;
    private boolean supportsInnerJoins;
    private boolean supportsFullOuterJoins;
    private boolean requiresCriteria;
    private int maxInSize = DEFAULT_MAX_IN_CRITERIA_SIZE;
    private int maxDependentInPredicates = DEFAULT_MAX_IN_CRITERIA_SIZE;
    private boolean copyLobs;
    private boolean supportsNativeQueries;
    private LinkedList<FunctionMethod> pushdownFunctionMethods = new LinkedList<FunctionMethod>();
    private String nativeProcedureName = "native"; //$NON-NLS-1$
    private String collationLocale;

    private TransactionSupport transactionSupport = TransactionSupport.XA;
    private String excludedCommonTableExpressionName;

    /**
     * Initialize the connector with supplied configuration
     */
    public void start() throws TranslatorException {
        if (!isSourceRequiredForCapabilities()) {
            initCapabilities(null);
        }
    }

    /**
     * Defines if the Connector is read-only connector
     * @return
     */
    @TranslatorProperty(display="Is Immutable",description="Is Immutable, True if the source never changes.",advanced=true)
    public boolean isImmutable() {
        return immutable;
    }

    public void setImmutable(boolean arg0) {
        this.immutable = arg0;
    }

    @TranslatorProperty(display="Copy LOBs",description="If true, returned LOBs will be copied, rather than streamed from the source",advanced=true)
    public boolean isCopyLobs() {
        return copyLobs;
    }

    public void setCopyLobs(boolean copyLobs) {
        this.copyLobs = copyLobs;
    }

    /**
     * Return a connection object from the given connection factory.
     *
     * The default implementation assumes a JCA {@link ConnectionFactory}.  Subclasses should override, if they use
     * another type of connection factory.
     *
     * @deprecated
     * @see #getConnection(Object, ExecutionContext)
     * @param factory
     * @return a connection
     * @throws TranslatorException
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public C getConnection(F factory) throws TranslatorException {
        if (factory == null) {
            return null;
        }
        if (factory instanceof ConnectionFactory) {
            try {
                return (C) ((ConnectionFactory)factory).getConnection();
            } catch (Exception e) {
                 throw new TranslatorException(DataPlugin.Event.TEIID60000, e);
            }
        }
        throw new AssertionError(factory.getClass().getName() + " is was not a ConnectionFactory implementation"); //$NON-NLS-1$
    }

    /**
     * Return a connection object from the given connection factory.
     *
     * The default implementation assumes a JCA {@link ConnectionFactory}.  Subclasses should override, if they use
     * another type of connection factory or wish to use the {@link ExecutionContext}.  By default calls {@link #getConnection(Object)}
     *
     * @param factory
     * @param executionContext null if this is a system request for a connection
     * @return a connection
     * @throws TranslatorException
     */
    public C getConnection(F factory,
            ExecutionContext executionContext) throws TranslatorException {
        return getConnection(factory);
    }

    /**
     * Closes a connection object from the given connection factory.
     *
     * The default implementation assumes a JCA {@link Connection}.  Subclasses should override, if they use
     * another type of connection.
     *
     * @param connection
     * @param factory
     */
    public void closeConnection(C connection, F factory) {
        if (connection == null) {
            return;
        }
        if (connection instanceof Connection) {
            try {
                ((Connection)connection).close();
            } catch (Exception e) {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Error closing"); //$NON-NLS-1$
            }
            return;
        }
        throw new AssertionError("A connection was created, but no implementation provided for closeConnection"); //$NON-NLS-1$
    }

    /**
     * Flag that indicates if a underlying source connection required for this execution factory to work
     * @return
     */
    public boolean isSourceRequired() {
        return sourceRequired;
    }

    public void setSourceRequired(boolean value) {
        this.sourceRequired = value;
    }

    /**
     * Flag that indicates if a underlying source connection required for this execution factory to return metadata
     * @return true if required
     */
    public boolean isSourceRequiredForMetadata() {
        if (sourceRequiredForMetadata == null) {
            //matches pre 8.1 behavior
            return sourceRequired;
        }
        //TODO we could also consider making this an annotation of the getMetadata call
        return sourceRequiredForMetadata;
    }

    /**
     * If true, the {@link #initCapabilities(Object)} method will be consulted prior
     * to determining the capabilities
     * @return
     */
    public boolean isSourceRequiredForCapabilities() {
        return false;
    }

    /**
     * Will be called by {@link #start()} with a null connection if a source connection is not {@link #isSourceRequiredForCapabilities()}
     * @param connection
     * @throws TranslatorException
     */
    public void initCapabilities(C connection) throws TranslatorException {

    }

    public void setSourceRequiredForMetadata(boolean sourceRequiredForMetadata) {
        this.sourceRequiredForMetadata = sourceRequiredForMetadata;
    }

    /**
     * Obtain a reference to the default LanguageFactory that can be used to construct
     * new language interface objects.  This is typically needed when modifying the language
     * objects passed to the connector or for testing when objects need to be created.
     */
    public LanguageFactory getLanguageFactory()  {
        return LanguageFactory.INSTANCE;
    }

    /**
     * Obtain a reference to the type facility, which can be used to perform many type
     * conversions supplied by the Connector API.
     */
    public TypeFacility getTypeFacility() {
        return TYPE_FACILITY;
    }

    /**
     * Create an execution object for the specified command
     * @param command the command
     * @param executionContext Provides information about the context that this command is
     * executing within, such as the identifiers for the command being executed
     * @param metadata Access to runtime metadata if needed to translate the command
     * @param connection connection factory object to the data source
     * @return An execution object that can use to execute the command
     */
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
        if (command instanceof Call) {
            Call obj = (Call)command;
            //TODO: our extension property support in designer makes ad-hoc properties impossible, so
            //we just match based upon name.  it would be better to have the metadatarepository/tooling add a teiid property
            //to explicitly set this proc as direct.
            //the other approach would be to addd a native system stored procedure, but that would require
            //special security semantics, whereas this proc can be secured on a schema basis
            if (supportsDirectQueryProcedure() && obj.getMetadataObject().getName().equals(getDirectQueryProcedureName())) {
                List<Argument> arguments = obj.getArguments();
                return createDirectExecution(arguments, command, executionContext, metadata, connection);
            }
            return createProcedureExecution((Call)command, executionContext, metadata, connection);
        }
        if (command instanceof QueryExpression) {
            return createResultSetExecution((QueryExpression)command, executionContext, metadata, connection);
        }
        return createUpdateExecution(command, executionContext, metadata, connection);
    }

    @SuppressWarnings("unused")
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
         throw new TranslatorException(DataPlugin.Event.TEIID60001, DataPlugin.Util.gs(DataPlugin.Event.TEIID60001, "createResultSetExecution")); //$NON-NLS-1$
    }

    @SuppressWarnings("unused")
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
         throw new TranslatorException(DataPlugin.Event.TEIID60001,  DataPlugin.Util.gs(DataPlugin.Event.TEIID60001, "createProcedureExecution")); //$NON-NLS-1$
    }

    @SuppressWarnings("unused")
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
         throw new TranslatorException(DataPlugin.Event.TEIID60001,  DataPlugin.Util.gs(DataPlugin.Event.TEIID60001, "createUpdateExecution")); //$NON-NLS-1$
    }

    @SuppressWarnings("unused")
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
         throw new TranslatorException(DataPlugin.Event.TEIID60001, DataPlugin.Util.gs(DataPlugin.Event.TEIID60001, "createDirectExecution")); //$NON-NLS-1$
    }

    /**
     * Get a MetadataProcessor for the translator to read the metadata.  Typically this will return a new instance.
     * @return
     */
    public MetadataProcessor<C> getMetadataProcessor() {
        return null;
    }

    /**
     * Support indicates connector can accept queries with SELECT DISTINCT
     * @since 3.1 SP2
     */
    @TranslatorProperty(display="Supports Select Distinct", description="True, if this connector supports SELECT DISTINCT", advanced=true)
    public boolean supportsSelectDistinct() {
        return supportsSelectDistinct;
    }

    public void setSupportsSelectDistinct(boolean supportsSelectDistinct) {
        this.supportsSelectDistinct = supportsSelectDistinct;
    }

    /**
     * Support indicates connector can accept expressions other than element
     * symbols in the SELECT clause.  Specific supports for the expression
     * type are still checked.
     * @since 6.1.0
     */
    public boolean supportsSelectExpression() {
        return false;
    }

    /**
     * Support indicates connector can accept groups with aliases
     * @since 3.1 SP2
     */
    public boolean supportsAliasedTable() {
        return false;
    }

    /**
     * Get the supported join criteria. A null return value will be treated
     * as {@link SupportedJoinCriteria#ANY}
     * @since 6.1.0
     */
    @TranslatorProperty(display="Supported Join Criteria", description="Returns one of any, theta, equi, or key", advanced=true)
    public SupportedJoinCriteria getSupportedJoinCriteria() {
        return supportedJoinCriteria;
    }

    public void setSupportedJoinCriteria(
            SupportedJoinCriteria supportedJoinCriteria) {
        this.supportedJoinCriteria = supportedJoinCriteria;
    }

    /**
     * Support indicates connector can accept inner or cross joins
     * @since 6.1.0
     */
    @TranslatorProperty(display="Supports Inner Joins", description="True, if this connector supports inner joins", advanced=true)
    public boolean supportsInnerJoins() {
        return supportsInnerJoins;
    }

    public void setSupportsInnerJoins(boolean supportsInnerJoins) {
        this.supportsInnerJoins = supportsInnerJoins;
    }

    /**
     * Support indicates connector can accept self-joins where a
     * group is joined to itself with aliases.  Connector must also support
     * {@link #supportsAliasedTable()}.
     * @since 3.1 SP2
     */
    public boolean supportsSelfJoins() {
        return false;
    }

    /**
     * Support indicates connector can accept left outer joins
     * @since 3.1 SP2
     */
    @TranslatorProperty(display="Supports Outer Joins", description="True, if this connector supports outer joins", advanced=true)
    public boolean supportsOuterJoins() {
        return supportsOuterJoins;
    }

    public void setSupportsOuterJoins(boolean supportsOuterJoins) {
        this.supportsOuterJoins = supportsOuterJoins;
    }

    /**
     * Support indicates connector can accept full outer joins
     * @since 3.1 SP2
     */
    @TranslatorProperty(display="Supports Full Outer Joins", description="True, if this connector supports full outer joins", advanced=true)
    public boolean supportsFullOuterJoins() {
        return supportsFullOuterJoins;
    }

    public void setSupportsFullOuterJoins(boolean supportsFullOuterJoins) {
        this.supportsFullOuterJoins = supportsFullOuterJoins;
    }

    /**
     * Support indicates connector can accept inline views (subqueries
     * in the FROM clause).
     * @since 4.1
     */
    public boolean supportsInlineViews() {
        return false;
    }

    /**
     * Support indicates connector accepts criteria of form (exp1 IS DISTINCT exp2)
     * @since 10.0
     */
    public boolean supportsIsDistinctCriteria() {
        return false;
    }

    /**
     * Support indicates connector accepts criteria of form (element = constant)
     * @since 3.1 SP2
     */
    public boolean supportsCompareCriteriaEquals() {
        return false;
    }

    /**
     * Support indicates connector accepts criteria of form (element &lt;=|&gt;= constant)
     * <br>The query engine will may pushdown queries containing &lt; or &gt; if NOT is also
     * supported.
     * @since 3.1 SP2
     */
    public boolean supportsCompareCriteriaOrdered() {
        return false;
    }

    /**
     * Support indicates connector accepts criteria of form (element &lt;|&gt; constant)
     */
    public boolean supportsCompareCriteriaOrderedExclusive() {
        return supportsCompareCriteriaOrdered();
    }

    /**
     * Support indicates connector accepts criteria of form (element LIKE constant)
     * @since 3.1 SP2
     */
    public boolean supportsLikeCriteria() {
        return false;
    }

    /**
     * Support indicates connector accepts criteria of form (element LIKE constant ESCAPE char)
     * @since 3.1 SP2
     */
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return false;
    }

    /**
     * Support indicates connector accepts criteria of form (element IN set)
     * @since 3.1 SP2
     */
    public boolean supportsInCriteria() {
        return false;
    }

    /**
     * Support indicates connector accepts IN criteria with a subquery on the right side
     * @since 4.0
     */
    public boolean supportsInCriteriaSubquery() {
        return false;
    }

    /**
     * Support indicates connector accepts criteria of form (element IS NULL)
     * @since 3.1 SP2
     */
    public boolean supportsIsNullCriteria() {
        return false;
    }

    /**
     * Support indicates connector accepts logical criteria connected by OR
     * @since 3.1 SP2
     */
    public boolean supportsOrCriteria() {
        return false;
    }

    /**
     * Support indicates connector accepts logical criteria NOT
     * @since 3.1 SP2
     */
    public boolean supportsNotCriteria() {
        return false;
    }

    /**
     * Support indicates connector accepts the EXISTS criteria
     * @since 4.0
     */
    public boolean supportsExistsCriteria() {
        return false;
    }

    /**
     * Support indicates connector accepts the quantified comparison criteria that
     * use SOME
     * @since 4.0
     */
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    /**
     * Support indicates connector accepts the quantified comparison criteria that
     * use ALL
     * @since 4.0
     */
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    /**
     * Support indicates connector accepts ORDER BY clause, including multiple elements
     * and ascending and descending sorts.
     * @since 3.1 SP2
     */
    @TranslatorProperty(display="Supports ORDER BY", description="True, if this connector supports ORDER BY", advanced=true)
    public boolean supportsOrderBy() {
        return supportsOrderBy;
    }

    public void setSupportsOrderBy(boolean supportsOrderBy) {
        this.supportsOrderBy = supportsOrderBy;
    }

    /**
     * Indicates the collation used for sorting
     */
    @TranslatorProperty(display="Collation Locale", description="The collation locale used by default for sorting.", advanced=true)
    public String getCollationLocale() {
        return collationLocale;
    }

    public void setCollationLocale(String collation) {
        this.collationLocale = collation;
    }

    /**
     * Support indicates connector accepts ORDER BY clause with columns not from the select
     * @since 6.2
     * @return
     */
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    /**
     * Returns the default null ordering
     * @since 7.1
     * @return the {@link NullOrder}
     */
    public NullOrder getDefaultNullOrder() {
        return NullOrder.UNKNOWN;
    }

    /**
     * Returns whether the database supports explicit null ordering.
     * @since 7.1
     * @return true if nulls first/last can be specified
     */
    public boolean supportsOrderByNullOrdering() {
        return false;
    }

    /**
     * Whether the source supports an explicit GROUP BY clause
     * @since 6.1
     */
    public boolean supportsGroupBy() {
        return false;
    }

    /**
     * Whether the source supports grouping only over a single table
     * @return
     */
    public boolean supportsOnlySingleTableGroupBy() {
        return false;
    }

    /**
     * Whether the source supports grouping with multiple distinct aggregates
     * @return
     */
    public boolean supportsGroupByMultipleDistinctAggregates() {
        return supportsGroupBy();
    }

    /**
     * Whether the source supports the HAVING clause
     * @since 6.1
     */
    public boolean supportsHaving() {
        return false;
    }

    /**
     * Support indicates connector can accept the SUM aggregate function
     * @since 3.1 SP2
     */
    public boolean supportsAggregatesSum() {
        return false;
    }

    /**
     * Support indicates connector can accept the AVG aggregate function
     * @since 3.1 SP2
     */
    public boolean supportsAggregatesAvg() {
        return false;
    }

    /**
     * Support indicates connector can accept the MIN aggregate function
     * @since 3.1 SP2
     */
    public boolean supportsAggregatesMin() {
        return false;
    }

    /**
     * Support indicates connector can accept the MAX aggregate function
     * @since 3.1 SP2
     */
    public boolean supportsAggregatesMax() {
        return false;
    }

    /**
     * Support indicates connector can accept the COUNT aggregate function
     * @since 3.1 SP2
     */
    public boolean supportsAggregatesCount() {
        return false;
    }

    /**
     * Support indicates connector can accept the COUNT(*) aggregate function
     * @since 3.1 SP2
     */
    public boolean supportsAggregatesCountStar() {
        return false;
    }

    /**
     * Support indicates connector can accept DISTINCT within aggregate functions
     * @since 3.1 SP2
     */
    public boolean supportsAggregatesDistinct() {
        return false;
    }

    /**
     * Support indicates connector can accept STDDEV_POP, STDDEV_VAR, VAR_POP, VAR_SAMP
     * @since 7.1
     */
    public boolean supportsAggregatesEnhancedNumeric() {
        return false;
    }

    /**
     * @return true if string_agg is supported
     * @since 8.4
     */
    public boolean supportsStringAgg() {
        return false;
    }

    /**
     *
     * @return true if the translator supports a simplified string_agg - listagg
     * @since 11.2
     */
    public boolean supportsListAgg() {
        return supportsStringAgg();
    }

    /**
     * Support indicates connector can accept scalar subqueries in the SELECT, WHERE, and
     * HAVING clauses
     * @since 4.0
     */
    public boolean supportsScalarSubqueries() {
        return false;
    }

    /**
     * Support indicates connector can accept correlated subqueries wherever subqueries
     * are accepted
     * @since 4.0
     */
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    /**
     * Support indicates connector can accept queries with searched CASE WHEN criteria ... END
     * @since 4.0
     */
    public boolean supportsSearchedCaseExpressions() {
        return false;
    }

    /**
     * Support indicates that the connector supports the UNION of two queries.
     * @since 4.2
     */
    public boolean supportsUnions() {
        return false;
    }

    /**
     * Support indicates that the connector supports an ORDER BY on a SetQuery.
     * @since 5.6
     */
    public boolean supportsSetQueryOrderBy() {
        return false;
    }

    /**
     * Support indicates that the connector supports an LIMIT/OFFSET on a SetQuery.
     * @since 10.0
     */
    public boolean supportsSetQueryLimitOffset() {
        return supportsRowLimit() || supportsRowOffset();
    }

    /**
     * Support indicates that the connector supports the INTERSECT of two queries.
     * @since 5.6
     */
    public boolean supportsIntersect() {
        return false;
    }

    /**
     * Support indicates that the connector supports the EXCEPT of two queries.
     * @since 5.6
     */
    public boolean supportsExcept() {
        return false;
    }

    /**
     * Get list of all supported function names.  Arithmetic functions have names like
     * &quot;+&quot;.
     * @see SourceSystemFunctions for a listing of system pushdown functions.  Note that
     * not all system functions are listed as some functions will use a common name
     * such as CONCAT vs. the || operator, and other functions will be rewritten and
     * not pushed down, such as SPACE.
     * <br><b>Note:</b> User defined functions should be specified fully qualified.
     * @since 3.1 SP3
     */
    public List<String> getSupportedFunctions() {
        return null;
    }

    /**
     * Get a list of {@link FunctionMethod}s that will be contributed to the SYS schema.
     * To avoid conflicts with system functions, the function name should contain a
     * qualifier - typically &lt;translator name&gt;.&lt;function name&gt;
     * @see ExecutionFactory#addPushDownFunction(String, String, String, String...)
     * @return
     */
    public List<FunctionMethod> getPushDownFunctions(){
        return pushdownFunctionMethods;
    }

    /**
     * Adds a pushdown function.
     * @param qualifier will be pre-pended to the name
     * @param name
     * @param returnType see {@link RUNTIME_NAMES} for type names
     * @param paramTypes see {@link RUNTIME_NAMES} for type names
     * @return the FunctionMethod created.
     */
    protected FunctionMethod addPushDownFunction(String qualifier, String name, String returnType, String...paramTypes) {
        FunctionMethod method = FunctionMethod.createFunctionMethod(qualifier + '.' + name, name, qualifier,
                returnType, paramTypes);
        method.setNameInSource(name);
        pushdownFunctionMethods.add(method);
        return method;
    }

    /**
     * Get the integer value representing the number of values allowed in an IN criteria
     * in the WHERE clause of a query
     * @since 5.0
     */
    @TranslatorProperty(display="Max number of IN predicate entries", advanced=true)
    public int getMaxInCriteriaSize() {
        return maxInSize;
    }

    public void setMaxInCriteriaSize(int maxInSize) {
        this.maxInSize = maxInSize;
    }

    /**
     * Get the integer value representing the max number of dependent IN predicates.
     * This may be used to split a single dependent value via OR, or multiple dependent values
     * via AND.
     */
    @TranslatorProperty(display="Max number of dependent IN predicates", advanced=true)
    public int getMaxDependentInPredicates() {
        return maxDependentInPredicates;
    }

    public void setMaxDependentInPredicates(int maxDependentInPredicates) {
        this.maxDependentInPredicates = maxDependentInPredicates;
    }

    /**
     * <p>Support indicates that the connector supports non-column expressions in GROUP BY, such as:
     *  <code>SELECT dayofmonth(theDate), COUNT(*) FROM table GROUP BY dayofmonth(theDate)</code>
     * @since 5.0
     */
    public boolean supportsFunctionsInGroupBy() {
        return false;
    }

    /**
     * Gets whether the connector can limit the number of rows returned by a query.
     * @since 5.0 SP1
     */
    public boolean supportsRowLimit() {
        return false;
    }

    /**
     * Gets whether the connector supports a SQL clause (similar to the LIMIT with an offset) that can return
     * result sets that start in the middle of the resulting rows returned by a query
     * @since 5.0 SP1
     */
    public boolean supportsRowOffset() {
        return false;
    }

    /**
     * The number of groups supported in the from clause.  Added for a Sybase limitation.
     * @since 5.6
     * @return the number of groups supported in the from clause, or -1 if there is no limit
     */
    public int getMaxFromGroups() {
        return DEFAULT_MAX_FROM_GROUPS;
    }

    public boolean supportsOnlyRelationshipStyleJoins() {
        return false;
    }

    /**
     * The number of columns supported in projected select clause.  Added for a postgresql limitation.
     * @since 12.1
     * @return the maximum number of columns in the projected select clause or -1 if there is no limit
     */
    public int getMaxProjectedColumns() {
        return DEFAULT_MAX_PROJECTED_COLUMNS;
    }

    /**
     * Whether the source prefers to use ANSI style joins.
     * @since 6.0
     */
    public boolean useAnsiJoin() {
        return false;
    }

    /**
     * Whether the source supports queries without criteria.
     * @since 6.0
     */
    @TranslatorProperty(display="Requries Criteria", description="True, if this connector requires criteria on source queries", advanced=true)
    public boolean requiresCriteria() {
        return requiresCriteria;
    }

    public void setRequiresCriteria(boolean requiresCriteria) {
        this.requiresCriteria = requiresCriteria;
    }

    /**
     * Whether the source supports {@link BatchedUpdates}
     * @since 6.0
     */
    public boolean supportsBatchedUpdates() {
        return false;
    }

    /**
     * Whether the source supports updates with multiple value sets
     * @since 6.0
     */
    public boolean supportsBulkUpdate() {
        return false;
    }

    /**
     * Support indicates that the connector can accept INSERTs with
     * values specified by a {@link SetQuery} or {@link Select}
     * @since 6.1
     */
    public boolean supportsInsertWithQueryExpression() {
        return false;
    }

    public static <T> T getInstance(Class<T> expectedType, String className, Collection<?> ctorObjs, Class<? extends T> defaultClass) throws TranslatorException {
        try {
            if (className == null) {
                if (defaultClass == null) {
                     throw new TranslatorException(DataPlugin.Event.TEIID60004, DataPlugin.Util.gs(DataPlugin.Event.TEIID60004));
                }
                return expectedType.cast(defaultClass.newInstance());
            }
            return expectedType.cast(ReflectionHelper.create(className, ctorObjs, Thread.currentThread().getContextClassLoader()));
        } catch (TeiidException e) {
             throw new TranslatorException(DataPlugin.Event.TEIID60005, e);
        } catch (IllegalAccessException e) {
             throw new TranslatorException(DataPlugin.Event.TEIID60005, e);
        } catch(InstantiationException e) {
             throw new TranslatorException(DataPlugin.Event.TEIID60005, e);
        }
    }

    /**
     * Implement to provide metadata to the metadata for use by the engine.  This is the
     * primary method of creating metadata for dynamic VDBs.
     * @param metadataFactory
     * @param conn may be null if the source is not required
     * @throws TranslatorException to indicate a recoverable error, otherwise a RuntimeException
     * @see #isSourceRequiredForMetadata()
     */
    public void getMetadata(MetadataFactory metadataFactory, C conn) throws TranslatorException {
        MetadataProcessor mp = getMetadataProcessor();
        if (mp != null) {
            PropertiesUtils.setBeanProperties(mp, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
            mp.process(metadataFactory, conn);
        }
    }

    /**
     * Indicates if LOBs are usable after the execution is closed.
     * This check is not performed for values that are already {@link ClobType} or {@link BlobType}
     * @return true if LOBs can be used after close
     * @since 7.2
     */
    public boolean areLobsUsableAfterClose() {
        return false;
    }

    /**
     * @return true if the WITH clause is supported
     * @since 7.2
     */
    public boolean supportsCommonTableExpressions() {
        return false;
    }

    /**
     * @return true if a recursive WITH clause item is supported
     * @since 8.9
     */
    public boolean supportsRecursiveCommonTableExpressions() {
        return false;
    }

    /**
     * @return true if the WITH clause can appear in subqueries
     * @since 8.12
     */
    public boolean supportsSubqueryCommonTableExpressions() {
        return supportsCommonTableExpressions();
    }

    /**
     * @return true if a correlated subquery can support a limit clause
     * @since 8.12
     */
    public boolean supportsCorrelatedSubqueryLimit() {
        return supportsCorrelatedSubqueries();
    }

    /**
     * @return true if Advanced OLAP operations are supported
     *  including the aggregate function filter clause.
     * @since 7.5
     */
    public boolean supportsAdvancedOlapOperations() {
        return false;
    }

    /**
     * @return true if Elementary OLAP operations are supported
     *  including window functions and inline window specifications that include
     *  simple expressions in partitioning and ordering
     * @since 7.5
     */
    public boolean supportsElementaryOlapOperations() {
        return false;
    }

    /**
     * @return true if the window frame clause is supported
     */
    public boolean supportsWindowFrameClause() {
        return supportsElementaryOlapOperations();
    }

    /**
     * @return true if ntile is supported.
     *   defaults to {@link #supportsElementaryOlapOperations()}
     * @since 11.1
     */
    public boolean supportsWindowFunctionNtile() {
        return supportsElementaryOlapOperations();
    }

    /**
     * @return true if all aggregates can have window function order by clauses.
     * @since 7.5
     */
    public boolean supportsWindowOrderByWithAggregates() {
        return supportsElementaryOlapOperations();
    }

    /**
     * @return true if distinct aggregates can be windowed function.
     * @since 7.6
     */
    public boolean supportsWindowDistinctAggregates() {
        return supportsElementaryOlapOperations();
    }

    /**
     * @return true if array_agg is supported
     * @since 7.5
     */
    public boolean supportsArrayAgg() {
        return false;
    }

    /**
     * @return true if the SIMILAR TO predicate is supported
     * @since 7.5
     */
    public boolean supportsSimilarTo() {
        return false;
    }

    /**
     * @return true if the LIKE_REGEX predicate is supported
     * @since 7.4
     */
    public boolean supportsLikeRegex() {
        return false;
    }

    /**
     * Used for fine grained control of convert/cast pushdown.  The {@link #getSupportedFunctions()} should
     * contain {@link SourceSystemFunctions#CONVERT}.  This method can then return false to indicate
     * a lack of specific support.  The engine will does not care about an unnecessary conversion
     * where fromType == toType.
     *
     * By default lob conversion is disabled.
     *
     * @param fromType @see RUNTIME_CODES
     * @param toType @see RUNTIME_CODES
     * @return true if the given conversion is supported.
     * @since 8.0
     */
    public boolean supportsConvert(int fromType, int toType) {
        if (fromType == RUNTIME_CODES.OBJECT || (fromType == RUNTIME_CODES.CLOB && toType != RUNTIME_CODES.JSON) || fromType == RUNTIME_CODES.XML
                || fromType == RUNTIME_CODES.BLOB || toType == RUNTIME_CODES.CLOB || toType == RUNTIME_CODES.XML
                || toType == RUNTIME_CODES.BLOB || (fromType == RUNTIME_CODES.GEOGRAPHY && !supportsGeographyType())) {
            return false;
        }
        return true;
    }

    /**
     * @return true if only Literal comparisons (equality, ordered, like, etc.) are supported for non-join conditions.
     * @since 8.0
     */
    public boolean supportsOnlyLiteralComparison() {
        return false;
    }

    /**
      * NOTE: The pushed independent tuples will not have been
     * converted to a unique set and may contain duplicates.
     * @return true if dependent join key pushdown is supported
     * @since 8.0
     */
    public boolean supportsDependentJoins() {
        return false;
    }

    /**
     * @return true if full dependent join pushdown is supported
     * @since 8.5
     */
    public boolean supportsFullDependentJoins() {
        return false;
    }

    public enum Format {
        NUMBER,
        DATE
    }

    /**
     * See also {@link #supportsFormatLiteral(String, Format)}
     * @return true if only literal formats are supports.
     */
    public boolean supportsOnlyFormatLiterals() {
        return false;
    }

    /**
     *
     * @param literal
     * @param format
     * @return true if the given Java format string is supported
     */
    public boolean supportsFormatLiteral(String literal, Format format) {
        return false;
    }

    /**
     * Refines subquery support.
     * @return true if subqueries are supported in the on clause.
     */
    public boolean supportsSubqueryInOn() {
        return true;
    }

    /**
     * Get the {@link CacheDirective} to control command caching.
     * <p>Use {@link Scope#NONE} to indicate to the engine that no caching should be performed by the engine.
     * <p>If cache parameters on the {@link CacheDirective} will be changed by the {@link Execution}, then
     * a new instance of a {@link CacheDirective} should be set each time.
     * @param command
     * @param executionContext
     * @param metadata
     * @throws TranslatorException
     */
    public CacheDirective getCacheDirective(Command command, ExecutionContext executionContext, RuntimeMetadata metadata) throws TranslatorException {
        return null;
    }

    /**
     * When forkable the engine may use a separate thread to interact with returned {@link Execution}.
     * @return true if {@link Execution}s can be called in separate threads from the processing thread
     */
    public boolean isForkable() {
        return true;
    }

    /**
     * @return True, if this translator's executions must complete in a single thread.
     */
    @TranslatorProperty(display="Thread Bound", description="True, if this translator's executions must complete in a single thread.", advanced=true)
    public boolean isThreadBound() {
        return threadBound;
    }

    /**
     * The engine uses array types for dependent joins and for array expression.
     * @return true if an array type is supported.
     */
    public boolean supportsArrayType() {
        return false;
    }

    /**
     * @return true if array type expressions can be projected
     */
    public boolean supportsSelectExpressionArrayType() {
        return supportsArrayType();
    }

    /**
     * True, if this translator supports execution of source specific commands unaltered through a direct procedure.
      * @deprecated
     * @see #supportsDirectQueryProcedure()
     * @return
     */
    @Deprecated
    @TranslatorProperty(display="Deprecated Property:Supports Direct Query Procedure", description="Deprecated Property, Use Supports Direct Query Procedure instead", advanced=true)
    final public boolean supportsNativeQueries() {
        return this.supportsNativeQueries;
    }

    /**
     * @deprecated
     * @see #setSupportsDirectQueryProcedure(boolean)
     */
    @Deprecated
    final public void setSupportsNativeQueries(boolean state) {
        this.supportsNativeQueries = state;
    }

    /**
     * True, if this translator supports execution of source specific commands unaltered through a direct procedure.
     * @return
     */
    @TranslatorProperty(display="Supports Direct Query Procedure", description="True, if this translator supports execution of source specific commands unaltered through a direct procedure", advanced=true)
    public boolean supportsDirectQueryProcedure() {
        return this.supportsNativeQueries;
    }

    public void setSupportsDirectQueryProcedure(boolean state) {
        this.supportsNativeQueries = state;
    }

    /**
     * Defines the name of the direct processing procedure. This metadata or signature
     * of the procedure is defined automatically.
     * @deprecated
     * @see #getDirectQueryProcedureName()
     * @return
     */
    @Deprecated
    @TranslatorProperty(display="Deprecated Property:Direct Query Procedure Name", description="Deprecated Property, use Direct Query Procedure Name", advanced=true)
    final public String getNativeQueryProcedureName() {
        return this.nativeProcedureName;
    }

    /**
     * @deprecated
     * @see #setDirectQueryProcedureName(String)
     */
    @Deprecated
    final public void setNativeQueryProcedureName(String name) {
        this.nativeProcedureName = name;
    }

    /**
     * Defines the name of the direct processing procedure. This metadata or signature
     * of the procedure is defined automatically.
     * @return
     */
    @TranslatorProperty(display="Direct Query Procedure Name", description="The name of the direct query procedure", advanced=true)
    public String getDirectQueryProcedureName() {
        return this.nativeProcedureName;
    }

    public void setDirectQueryProcedureName(String name) {
        this.nativeProcedureName = name;
    }

    /**
     * @return true if only correlated subqueries are supported.
     */
    public boolean supportsOnlyCorrelatedSubqueries() {
        return false;
    }

    /**
     * @return true if the translator support SELECT without a FROM clause
     */
    public boolean supportsSelectWithoutFrom() {
        return false;
    }

    /**
     * @return true if the translator support GROUP BY ROLLUP
     */
    public boolean supportsGroupByRollup() {
        return false;
    }

    /**
     * @return true if order by is supported over a grouping with a rollup, cube, etc.
     */
    public boolean supportsOrderByWithExtendedGrouping() {
        return supportsOrderBy();
    }

    public void setThreadBound(boolean threadBound) {
        this.threadBound = threadBound;
    }

    /**
     * True if only a single value is returned for the update count.
     * This overrides the default expectation of an update count array
     * for bulk/batch commands.  It is expected that every command
     * is successful.
     * @return
     */
    public boolean returnsSingleUpdateCount() {
        return false;
    }

    /**
     * Return true if the source has columns marked with the teiid_rel:partial that
     * can return more rows than specified by a filter if the column is also projected.
     * This most closely matches the semantics of ldap queries with multi-valued
     * attributes marked as partial.
     * <br>When true, the following supports cannot also be true:
     * <ul>
     *   <li>supportsOuterJoins()
     *   <li>supportsFullOuterJoins()
     *   <li>supportsInlineViews()
     *   <li>supportsIntersect()
     *   <li>supportsExcept()
     *   <li>supportsSelectExpression()
     *   <li>supportsUnions()
     *   <li>supportsSelectDistinct()
     *   <li>supportsGroupBy()
     * </ul>
     * @return
     */
    public boolean supportsPartialFiltering() {
        return false;
    }

    /**
     * If dependent join predicates should use literals that are marked as bind eligible.
     */
    public boolean useBindingsForDependentJoin() {
        return true;
    }

    /**
     * The required escape character or null if all are supported.
     * @return
     */
    public Character getRequiredLikeEscape() {
        return null;
    }

    /**
     * If a scalar subquery can be projected.
     * @return
     */
    public boolean supportsScalarSubqueryProjection() {
        return supportsScalarSubqueries();
    }

    @TranslatorProperty(display="Transaction Support", description="The level of transaction support. Used by the engine to determine if a transaction is needed for autoCommit mode.", advanced=true)
    public TransactionSupport getTransactionSupport() {
        if (this.isImmutable()) {
            return TransactionSupport.NONE;
        }
        return transactionSupport;
    }

    public void setTransactionSupport(TransactionSupport transactionSupport) {
        this.transactionSupport = transactionSupport;
    }

    @TranslatorProperty(display="Excluded Common Table Expression Name", description="Set if the source won't support the given common table expression name.", advanced=true)
    public String getExcludedCommonTableExpressionName() {
        return excludedCommonTableExpressionName;
    }

    public void setExcludedCommonTableExpressionName(
            String excludedCommonTableExpressionName) {
        this.excludedCommonTableExpressionName = excludedCommonTableExpressionName;
    }

    /**
     *
     * @return true if the source supports lateral join
     */
    public boolean supportsLateralJoin() {
        return false;
    }

    /**
     *
     * @return true if the source supports lateral join conditions
     */
    public boolean supportsLateralJoinCondition() {
        return supportsLateralJoin();
    }

    /**
     *
     * @return true if lateral joins are restricted to only procedures / table valued functions
     */
    public boolean supportsOnlyLateralJoinProcedure() {
        return false;
    }

    /**
     *
     * @return
     */
    public boolean supportsProcedureTable() {
        return false;
    }

    /**
     * @return true if the source supports upsert
     */
    public boolean supportsUpsert() {
        return false;
    }

    /**
     * @return true if the source supports only timestamp add literals
     */
    public boolean supportsOnlyTimestampAddLiteral() {
        return false;
    }

    /**
     * @return true if percent_rank is supported.
     *   defaults to {@link #supportsElementaryOlapOperations()}
     * @since 11.1
     */
    public boolean supportsWindowFunctionPercentRank() {
        return supportsElementaryOlapOperations();
    }

    /**
     * @return true if cume_dist is supported.
     *   defaults to {@link #supportsElementaryOlapOperations()}
     * @since 11.1
     */
    public boolean supportsWindowFunctionCumeDist() {
        return supportsElementaryOlapOperations();
    }

    /**
     * @return true if nth_value is supported.
     *   defaults to {@link #supportsElementaryOlapOperations()}
     * @since 11.1
     */
    public boolean supportsWindowFunctionNthValue() {
        return supportsElementaryOlapOperations();
    }

    /**
     *
     * @since 11.2
     * @return true if multiple executions may be open against a single connection at a time
     */
    public boolean supportsMultipleOpenExecutions() {
        return true;
    }

    /**
     * @since 11.2
     * @return true if the translator supports a specific count aggregate returning a long value
     *
     * {@link #supportsAggregatesCount()} will be consulted by the engine, this capability
     * only affects the name of the pushed count function if a long value is expected
     */
    public boolean supportsAggregatesCountBig() {
        return false;
    }

    /**
     * If the geography type is supported by the standard ST_ geospatial functions
     * @since 11.2
     * @return true if the translator supports the geography type
     */
    public boolean supportsGeographyType() {
        return false;
    }

    /**
     * Return true if the translator supports expressions as procedure paramters.
     * @since 13.0
     * @return
     */
    public boolean supportsProcedureParameterExpression() {
        return false;
    }
}
