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
import org.teiid.metadata.RuntimeMetadata;



/**
 * <p>The primary entry point for a Translator.  This class should be extended by the custom translator writer.</p>
 * 
 * The deployer instantiates this class through reflection. So it is important to have no-arg constructor. Once constructed
 * the "start" method is called. This class represents the basic capabilities of the translator.
 */
public class ExecutionFactory implements TranslatorCapabilities{

	public static final int DEFAULT_MAX_FROM_GROUPS = -1;

	private static final TypeFacility TYPE_FACILITY = new TypeFacility();
	
	private boolean immutable = false;
	private boolean exceptionOnMaxRows = false;
	private int maxResultRows = DEFAULT_MAX_FROM_GROUPS;
	private boolean xaCapable;
	private boolean sourceRequired = true;
	
	/**
	 * Initialize the connector with supplied configuration
	 */
	public void start() throws TranslatorException {
	}
	    
	/**
	 * Defines if the Connector is read-only connector 
	 * @return
	 */
	@TranslatorProperty(name="immutable", display="Is Immutable",description="Is Immutable, True if the source never changes.",advanced=true, defaultValue="false")
	public boolean isImmutable() {
		return immutable;
	}
	
	public void setImmutable(boolean arg0) {
		this.immutable = arg0;
	}	
	
	/**
	 * Throw exception if there are more rows in the result set than specified in the MaxResultRows setting.
	 * @return
	 */
	@TranslatorProperty(name="exception-on-max-rows", display="Exception on Exceeding Max Rows",description="Indicates if an Exception should be thrown if the specified value for Maximum Result Rows is exceeded; else no exception and no more than the maximum will be returned",advanced=true, defaultValue="true")
	public boolean isExceptionOnMaxRows() {
		return exceptionOnMaxRows;
	}
	
	public void setExceptionOnMaxRows(boolean arg0) {
		this.exceptionOnMaxRows = arg0;
	}

	/**
	 * Maximum result set rows to fetch
	 * @return
	 */
	@TranslatorProperty(name="max-result-rows", display="Maximum Result Rows", description="Maximum Result Rows allowed", advanced=true, defaultValue="-1")
	public int getMaxResultRows() {
		return maxResultRows;
	}

	public void setMaxResultRows(int arg0) {
		this.maxResultRows = arg0;
	}
	
	/**
	 * Shows the XA transaction capability of the Connector.
	 * @return
	 */
	@TranslatorProperty(name="xa-capable", display="Is XA Capable", description="True, if this connector supports XA Transactions", defaultValue="false")
	public boolean isXaCapable() {
		return xaCapable;
	}

	public void setXaCapable(boolean arg0) {
		this.xaCapable = arg0;
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
	public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory) throws TranslatorException {
		if (command instanceof QueryExpression) {
			return createResultSetExecution((QueryExpression)command, executionContext, metadata, connectionFactory);
		}
		if (command instanceof Call) {
			return createProcedureExecution((Call)command, executionContext, metadata, connectionFactory);
		}
		return createUpdateExecution(command, executionContext, metadata, connectionFactory);
	}

	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws TranslatorException {
		throw new TranslatorException("Unsupported Execution"); //$NON-NLS-1$
	}

	public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws TranslatorException {
		throw new TranslatorException("Unsupported Execution");//$NON-NLS-1$
	}

	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws TranslatorException {
		throw new TranslatorException("Unsupported Execution");//$NON-NLS-1$
	}   
	
	@Override
    public boolean supportsSelectDistinct() {
        return false;
    }

    @Override
    public boolean supportsAliasedGroup() {
        return false;
    }

    @Override
    public boolean supportsSelfJoins() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsBetweenCriteria() {
        return false;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return false;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return false;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return false;
    }

    @Override
    public boolean supportsInCriteria() {
        return false;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
        return false;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return false;
    }

    @Override
    public boolean supportsOrCriteria() {
        return false;
    }

    @Override
    public boolean supportsNotCriteria() {
        return false;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    @Override
    public boolean supportsOrderBy() {
        return false;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return false;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return false;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return false;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return false;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return false;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return false;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
        return false;
    }

    @Override
    public boolean supportsScalarSubqueries() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsCaseExpressions() {
        return false;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return false;
    }

    @Override
    public List<String> getSupportedFunctions() {
        return null;
    }

    public boolean supportsInlineViews() {
        return false;
    }
    
    @Override
    public boolean supportsUnions() {
        return false;
    }

    @Override
    public int getMaxInCriteriaSize() {
        return DEFAULT_MAX_FROM_GROUPS;
    }
    
    @Override
    public boolean supportsFunctionsInGroupBy() {
        return false;
    }
    
    @Override
    public boolean supportsRowLimit() {
        return false;
    }

    @Override
    public boolean supportsRowOffset() {
        return false;
    }

    @Override
    public int getMaxFromGroups() {
        return DEFAULT_MAX_FROM_GROUPS; //-1 indicates no max
    }

    @Override
    public boolean supportsExcept() {
        return false;
    }

    @Override
    public boolean supportsIntersect() {
        return false;
    }

    @Override
    public boolean supportsSetQueryOrderBy() {
        return false;
    }    
    
    @Override
    public boolean useAnsiJoin() {
    	return false;
    }

    @Override
    public boolean requiresCriteria() {
    	return false;
    }

	@Override
	public boolean supportsBatchedUpdates() {
		return false;
	}

	@Override
	public boolean supportsGroupBy() {
		return false;
	}

	@Override
	public boolean supportsHaving() {
		return false;
	}

	@Override
	public boolean supportsInnerJoins() {
		return false;
	}

	@Override
	public boolean supportsSelectExpression() {
		return false;
	}
	
	@Override
	public SupportedJoinCriteria getSupportedJoinCriteria() {
		return SupportedJoinCriteria.ANY;
	}
	
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return false;
	}

	@Override
	public boolean supportsInsertWithQueryExpression() {
		return false;
	}
	
	@Override
	public boolean supportsBulkUpdate() {
		return false;
	}
	
	@Override
	public boolean supportsOrderByUnrelated() {
		return false;
	}	
	
}
