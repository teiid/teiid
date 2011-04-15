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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionFactory;

import org.teiid.core.TeiidException;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.LanguageFactory;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.metadata.TableStats;



/**
 * <p>The primary entry point for a Translator.  This class should be extended by the custom translator writer.</p>
 * 
 * The deployer instantiates this class through reflection. So it is important to have no-arg constructor. Once constructed
 * the "start" method is called. This class represents the basic capabilities of the translator.
 */
public class ExecutionFactory<F, C> {
	
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
	public static final int DEFAULT_MAX_IN_CRITERIA_SIZE = -1;

	private static final TypeFacility TYPE_FACILITY = new TypeFacility();
	
	/*
	 * Managed execution properties
	 */
	private boolean immutable;
	private boolean sourceRequired = true;
	
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
	
	/**
	 * Initialize the connector with supplied configuration
	 */
	@SuppressWarnings("unused")
	public void start() throws TranslatorException {
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
	
	/**
	 * Return a connection object from the given connection factory.
	 * 
	 * The default implementation assumes a JCA {@link ConnectionFactory}.  Subclasses should override, if they use 
	 * another type of connection factory.
	 * 
	 * @param factory
	 * @return
	 * @throws TranslatorException
	 */
	@SuppressWarnings("unchecked")
	public C getConnection(F factory) throws TranslatorException {
		if (factory == null) {
			return null;
		}
		if (factory instanceof ConnectionFactory) {
			try {
				return (C) ((ConnectionFactory)factory).getConnection();
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}
		}
		throw new AssertionError("A connection factory was supplied, but no implementation was provided getConnection"); //$NON-NLS-1$
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
			} catch (ResourceException e) {
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
		if (command instanceof QueryExpression) {
			return createResultSetExecution((QueryExpression)command, executionContext, metadata, connection);
		}
		if (command instanceof Call) {
			return createProcedureExecution((Call)command, executionContext, metadata, connection);
		}
		return createUpdateExecution(command, executionContext, metadata, connection);
	}

	@SuppressWarnings("unused")
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
		throw new TranslatorException("Unsupported Execution"); //$NON-NLS-1$
	}

	@SuppressWarnings("unused")
	public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
		throw new TranslatorException("Unsupported Execution");//$NON-NLS-1$
	}

	@SuppressWarnings("unused")
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, C connection) throws TranslatorException {
		throw new TranslatorException("Unsupported Execution");//$NON-NLS-1$
	}   
	
    /** 
     * Support indicates connector can accept queries with SELECT DISTINCT
     * @since 3.1 SP2 
     */
	@TranslatorProperty(display="Supports Select Distinct", description="True, if this connector supports SELECT DISTINCT", advanced=true)
    public final boolean supportsSelectDistinct() {
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
    public final SupportedJoinCriteria getSupportedJoinCriteria() {
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
    public final boolean supportsInnerJoins() {
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
    public final boolean supportsOuterJoins() {
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
    public final boolean supportsFullOuterJoins() {
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
     * Support indicates connector accepts criteria of form (element BETWEEN constant AND constant)
     * <br>NOT CURRENTLY USED - between is rewritten as compound compare criteria
     * @since 4.0
     */
    public boolean supportsBetweenCriteria() {
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
    public final boolean supportsOrderBy() {
    	return supportsOrderBy;
    }
    
    public void setSupportsOrderBy(boolean supportsOrderBy) {
		this.supportsOrderBy = supportsOrderBy;
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
	 * Returns whether the database supports explicit join ordering.
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
     * Support indicates connector can accept queries with non-searched
     * CASE <expression> WHEN <expression> ... END
     * <br>NOT CURRENTLY USED - case is pushed down as searched case
     * @since 4.0
     */
    public boolean supportsCaseExpressions() {
    	return false;
    }

    /**
     * Support indicates connector can accept queries with searched CASE WHEN <criteria> ... END
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
     * @since 3.1 SP3    
     */        
    public List<String> getSupportedFunctions() {
    	return null;
    }
    
    public List<FunctionMethod> getPushDownFunctions(){
    	return Collections.emptyList();
    }
    
    /**
     * Get the integer value representing the number of values allowed in an IN criteria
     * in the WHERE clause of a query
     * @since 5.0
     */
	@TranslatorProperty(display="Max number of IN predicate entries", advanced=true)
    public final int getMaxInCriteriaSize() {
        return maxInSize;
    }
	
	public void setMaxInCriteriaSize(int maxInSize) {
		this.maxInSize = maxInSize;
	}
	
    /**
     * Get the integer value representing the number of values allowed in an IN criteria
     * in the WHERE clause of a query
     * @since 5.0
     */
	@TranslatorProperty(display="Max number of dependent values across all IN predicates", advanced=true)
	public int getMaxDependentInPredicates() {
		return maxDependentInPredicates;
	}
	
	public void setMaxDependentInPredicates(int maxDependentInPredicates) {
		this.maxDependentInPredicates = maxDependentInPredicates;
	}

    /**
     * <p>Support indicates that the connector supports functions in GROUP BY, such as:
     *  <code>SELECT dayofmonth(theDate), COUNT(*) FROM table GROUP BY dayofmonth(theDate)</code></p>
     *  
     * <br>NOT CURRENTLY USED - group by expressions create an inline view for pushdown
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
    public final boolean requiresCriteria() {
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
    
    /**
     * Support indicates that the connector can accept INSERTs
     * with values specified by an {@link Iterator}
     * @since 7.1
     * @return
     */
    public boolean supportsInsertWithIterator() {
    	return false;
    }
	
    public static <T> T getInstance(Class<T> expectedType, String className, Collection<?> ctorObjs, Class<? extends T> defaultClass) throws TranslatorException {
    	try {
	    	if (className == null) {
	    		if (defaultClass == null) {
	    			throw new TranslatorException("Neither class name nor default class specified to create an instance"); //$NON-NLS-1$
	    		}
	    		return expectedType.cast(defaultClass.newInstance());
	    	}
	    	return expectedType.cast(ReflectionHelper.create(className, ctorObjs, Thread.currentThread().getContextClassLoader()));
		} catch (TeiidException e) {
			throw new TranslatorException(e);
		} catch (IllegalAccessException e) {
			throw new TranslatorException(e);
		} catch(InstantiationException e) {
			throw new TranslatorException(e);
		}    	
    } 
    
    /**
     * Implement to provide metadata to the metadata for use by the engine.  This is the 
     * primary method of creating metadata for dynamic VDBs.
     * @param metadataFactory
     * @param conn
     * @throws TranslatorException
     */
    public void getMetadata(MetadataFactory metadataFactory, C conn) throws TranslatorException {
    	
    }
    
	public boolean updateTableStats(Table table, TableStats stats, C conn) throws TranslatorException {
		return false;
	}
	
	public boolean updateColumnStats(Column column, ColumnStats stats, C conn) throws TranslatorException {
		return false;
	}
    
    /**
     * Indicates if LOBs are usable after the execution is closed.
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
}
