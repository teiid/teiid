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
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;

/**
 * Loopback translator.
 */
@Translator(name="loopback", description="A translator for testing, that returns mock data")
public class LoopbackExecutionFactory extends ExecutionFactory<Object, Object> {

	private int waitTime = 0;
	private int rowCount = 1;
	private boolean throwError = false;
	private long pollIntervalInMilli = -1;
	private boolean incrementRows = false;
	private boolean disableCapabilities = false;
	private int charValueSize = 10;
	
	public LoopbackExecutionFactory() {
		setSupportsFullOuterJoins(true);
		setSupportsOrderBy(true);
		setSupportsOuterJoins(true);
		setSupportsSelectDistinct(true);
		setSupportsInnerJoins(true);
	}
	
	@TranslatorProperty(display="Size of values for CLOB, VARCHAR, etc.", advanced=true)
	public int getCharacterValuesSize() {
		return charValueSize;
	}
	
	public void setCharacterValuesSize(int charValSize){
		this.charValueSize = charValSize;
	}
	
	@TranslatorProperty(display="If set to true each value in each column is being incremented with each row", advanced=true)
	public boolean getIncrementRows() {
		return incrementRows;
	}	
	
	public void setIncrementRows(boolean incrementRows) {
		this.incrementRows = incrementRows;
	}	
	
	@TranslatorProperty(display="If set to true all translator capabilities will be disabled", advanced=false)
	public boolean getDisableCapabilities() {
		return disableCapabilities;
	}	
	
	public void setDisableCapabilities(boolean disableCapabilities) {
		this.disableCapabilities = !disableCapabilities;
	}	
	
	@Override
	public Object getConnection(Object factory) throws TranslatorException {
		return null;
	}
	
	@TranslatorProperty(display="Max Random Wait Time", advanced=true)
	public int getWaitTime() {
		return waitTime;
	}
	
	public void setWaitTime(int waitTime) {
		this.waitTime = waitTime;
	}
	
	@TranslatorProperty(display="Rows Per Query", advanced=true)
	public int getRowCount() {
		return rowCount;
	}
	
	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
	
	@TranslatorProperty(display="Always Throw Error")
	public boolean isThrowError() {
		return this.throwError;
	}
	
	public void setThrowError(boolean error) {
		this.throwError = error;
	}
	
	@TranslatorProperty(display="Poll interval if using a Asynchronous Connector")
	public long getPollIntervalInMilli() {
		return this.pollIntervalInMilli;
	}
	
	public void setPollIntervalInMilli(long intervel) {
		this.pollIntervalInMilli = intervel;
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
	}

    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection)
    		throws TranslatorException {
        return new LoopbackExecution(command, this);
    }   
    
	@Override
	public boolean isSourceRequired() {
		return false;
	}    
	
	@Override
    public List<String> getSupportedFunctions() {
        List<String> functions = Arrays.asList(new String[] {
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
    	return disableCapabilities;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsAliasedTable() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsInCriteria() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsNotCriteria() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsOrCriteria() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsScalarSubqueries() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsSelfJoins() {
        return disableCapabilities;
    }
    
    @Override
    public boolean supportsInlineViews() {
        return disableCapabilities;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return disableCapabilities;
    }
    @Override
    public boolean supportsRowLimit() {
        return disableCapabilities;
    }
    
    @Override
    public boolean supportsSelectExpression() {
    	return disableCapabilities;
    }
        
    @Override
    public boolean supportsSetQueryOrderBy() {
    	return disableCapabilities;
    }
    
    @Override
    public boolean supportsUnions() {
    	return true;
    }
    
    @Override
    public boolean supportsCompareCriteriaOrdered() {
    	return disableCapabilities;
    }
    
    @Override
    public boolean supportsExcept() {
    	return disableCapabilities;
    }
    
    @Override
    public boolean supportsHaving() {
    	return disableCapabilities;
    }
    
    @Override
    public boolean supportsIntersect() {
    	return disableCapabilities;
    }


}
