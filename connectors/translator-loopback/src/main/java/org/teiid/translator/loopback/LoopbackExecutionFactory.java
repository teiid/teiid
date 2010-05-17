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

package org.teiid.translator.loopback;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorProperty;

/**
 * Loopback translator.
 */
public class LoopbackExecutionFactory extends ExecutionFactory {

	private int waitTime = 0;
	private int rowCount = 1;
	private boolean throwError = false;
	private long pollIntervalInMilli = -1;
	
	public LoopbackExecutionFactory() {
		setSupportsFullOuterJoins(true);
		setSupportsOrderBy(true);
		setSupportsOuterJoins(true);
		setSupportsSelectDistinct(true);
		setSupportsInnerJoins(true);
	}
	
	@TranslatorProperty(display="Max Random Wait Time", advanced=true)
	public int getWaitTime() {
		return waitTime;
	}
	
	public void setWaitTime(Integer waitTime) {
		this.waitTime = waitTime.intValue();
	}
	
	@TranslatorProperty(display="Rows Per Query", advanced=true)
	public int getRowCount() {
		return rowCount;
	}
	
	public void setRowCount(Integer rowCount) {
		this.rowCount = rowCount;
	}
	
	@TranslatorProperty(display="Always Throw Error")
	public boolean isThrowError() {
		return this.throwError;
	}
	
	public void setThrowError(Boolean error) {
		this.throwError = error.booleanValue();
	}
	
	@TranslatorProperty(display="Poll interval if using a Asynchronous Connector")
	public long getPollIntervalInMilli() {
		return this.pollIntervalInMilli;
	}
	
	public void setPollIntervalInMilli(Long intervel) {
		this.pollIntervalInMilli = intervel.longValue();
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
	}

    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionfactory)
    		throws TranslatorException {
        return new LoopbackExecution(command, this);
    }   
    
	@Override
	public boolean isSourceRequired() {
		return false;
	}    
	
	@Override
    public List getSupportedFunctions() {
        List functions = Arrays.asList(new String[] {
            "+", "-", "*", "/", "abs", "acos", "asin", "atan", "atan2", "ceiling", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$
            "bitand", "bitnot", "bitor", "bitxor", "cos", "cot", "degrees", "cos", "cot", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "degrees", "exp", "floor", "log", "log10", "mod", "pi", "power", "radians",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "round", "sign", "sin", "sqrt", "tan",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            "ascii", "chr", "char", "concat", "initcap", "insert", "lcase", "left", "length", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "locate", "lower", "lpad", "ltrim", "repeat", "replace", "right", "rpad", "rtrim", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "substring", "translate", "ucase", "upper",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            "curdate", "curtime", "now", "dayname", "dayofmonth", "dayofweek", "dayofyear",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
            "hour", "minute", "month", "monthname", "quarter", "second", "timestampadd",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
            "timestampdiff", "week", "year", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            "cast", "convert", "ifnull", "nvl"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        });
        return functions;
    }
    
    @Override
    public boolean supportsGroupBy() {
    	return true;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
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
    public boolean supportsAliasedTable() {
        return true;
    }

    @Override
    public boolean supportsBetweenCriteria() {
        return true;
    }

    @Override
    public boolean supportsCaseExpressions() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
        return true;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
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
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return true;
    }

    @Override
    public boolean supportsScalarSubqueries() {
        return true;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return true;
    }

    @Override
    public boolean supportsSelfJoins() {
        return true;
    }
    
    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return true;
    }
    @Override
    public boolean supportsRowLimit() {
        return true;
    }
    
    @Override
    public boolean supportsSelectExpression() {
    	return true;
    }
        
    @Override
    public boolean supportsSetQueryOrderBy() {
    	return true;
    }
    
    @Override
    public boolean supportsUnions() {
    	return true;
    }
    
    @Override
    public boolean supportsCompareCriteriaOrdered() {
    	return true;
    }
    
    @Override
    public boolean supportsExcept() {
    	return true;
    }
    
    @Override
    public boolean supportsHaving() {
    	return true;
    }
    
    @Override
    public boolean supportsIntersect() {
    	return true;
    }	
}
