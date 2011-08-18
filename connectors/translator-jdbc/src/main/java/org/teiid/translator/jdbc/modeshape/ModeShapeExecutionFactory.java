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

package org.teiid.translator.jdbc.modeshape;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Comparison;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.Not;
import org.teiid.language.Comparison.Operator;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
/** 
 * Translator class for accessing the ModeShape JCR repository.  
 */
@Translator(name="modeshape", description="A translator for the open source Modeshape JCR Repository")
public class ModeShapeExecutionFactory extends JDBCExecutionFactory {
	
	private static final String JCR = "JCR"; //$NON-NLS-1$
	private static final String JCR_REFERENCE = "JCR_REFERENCE";//$NON-NLS-1$
	private static final String JCR_CONTAINS = "JCR_CONTAINS";//$NON-NLS-1$
	private static final String JCR_ISSAMENODE = "JCR_ISSAMENODE";//$NON-NLS-1$
	private static final String JCR_ISDESCENDANTNODE = "JCR_ISDESCENDANTNODE";//$NON-NLS-1$
	private static final String JCR_ISCHILDNODE = "JCR_ISCHILDNODE";//$NON-NLS-1$
	
	public ModeShapeExecutionFactory() {
		setDatabaseVersion("2.0"); //$NON-NLS-1$
		setUseBindVariables(false);
	}
	
    @Override
    public void start() throws TranslatorException {
        super.start();
        
		registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UpperCase")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LCASE,new AliasModifier("LowerCase")); //$NON-NLS-1$
        
		registerFunctionModifier(JCR_ISCHILDNODE, new IdentifierFunctionModifier()); 
		registerFunctionModifier(JCR_ISDESCENDANTNODE, new IdentifierFunctionModifier()); 
		registerFunctionModifier(JCR_ISSAMENODE, new IdentifierFunctionModifier()); 
		registerFunctionModifier(JCR_REFERENCE, new IdentifierFunctionModifier()); 
		registerFunctionModifier(JCR_CONTAINS, new IdentifierFunctionModifier());
		
		addPushDownFunction(JCR, JCR_ISCHILDNODE, BOOLEAN, STRING, STRING);
		addPushDownFunction(JCR, JCR_ISDESCENDANTNODE, BOOLEAN, STRING, STRING);
		addPushDownFunction(JCR, JCR_ISSAMENODE, BOOLEAN, STRING, STRING);
		addPushDownFunction(JCR, JCR_CONTAINS, BOOLEAN, STRING, STRING);
		addPushDownFunction(JCR, JCR_REFERENCE, BOOLEAN, STRING);
		
    	LogManager.logTrace(LogConstants.CTX_CONNECTOR, "ModeShape Translator Started"); //$NON-NLS-1$
     }    
    
    @Override
    public String translateLiteralDate(Date dateValue) {
    	return "CAST('" + formatDateValue(dateValue) + "' AS DATE)"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
    	return "CAST('" + formatDateValue(timeValue) + "' AS DATE)"; //$NON-NLS-1$//$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
    	return "CAST('" + formatDateValue(timestampValue) + "' AS DATE)"; //$NON-NLS-1$//$NON-NLS-2$  
    }
    
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
    	return "CAST('" + booleanValue.toString() + "' AS BOOLEAN)"; //$NON-NLS-1$//$NON-NLS-2$ 
    }
    
    @Override
    public List<String> getSupportedFunctions() {
		List<String> supportedFunctions = new ArrayList<String>();
		supportedFunctions.addAll(super.getSupportedFunctions());
		supportedFunctions.add(SourceSystemFunctions.UCASE); 
		supportedFunctions.add(SourceSystemFunctions.LCASE); 
		supportedFunctions.add(SourceSystemFunctions.LENGTH);
		return supportedFunctions;
    }
    
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (obj instanceof Comparison) {
    		Comparison compare = (Comparison)obj;
    		if (compare.getLeftExpression().getType() == TypeFacility.RUNTIME_TYPES.BOOLEAN 
    				&& compare.getLeftExpression() instanceof Function 
    				&& compare.getRightExpression() instanceof Literal) {
    			boolean isTrue = Boolean.TRUE.equals(((Literal)compare.getRightExpression()).getValue());
    			if ((isTrue && compare.getOperator() == Operator.EQ) || (!isTrue && compare.getOperator() == Operator.NE)) {
    				return Arrays.asList(compare.getLeftExpression());
    			}
    			if ((!isTrue && compare.getOperator() == Operator.EQ) || (isTrue && compare.getOperator() == Operator.NE)) {
    				return Arrays.asList("NOT ", compare.getLeftExpression()); //$NON-NLS-1$
    			}
    		}
    	} else if (obj instanceof Not) {
    		Not not = (Not)obj;
    		return Arrays.asList("NOT ", not.getCriteria()); //$NON-NLS-1$
    	}
    	return super.translate(obj, context);
    }
        
    @Override
    public boolean useBindVariables() {
		return false;
	}
    
    @Override
    public boolean supportsAggregatesAvg() {
    	return false;
    }
    
    @Override
    public boolean supportsAggregatesCountStar() {
    	return false;
    }
    
    @Override
    public boolean supportsAggregatesCount() {
    	return false;
    }

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return false;
    }

    @Override
    public boolean supportsAggregatesMax() {
    	return false;
    }

    @Override
    public boolean supportsAggregatesMin() {
    	return false;
    }
    
    @Override
    public boolean supportsAggregatesSum() {
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
    public boolean supportsSelectExpression() {
    	return false;
    }
    
    @Override
    public boolean supportsCaseExpressions() {
    	return false;
    }
    
    @Override
    public boolean supportsCorrelatedSubqueries() {
    	return false;
    }
    
    @Override
    public boolean supportsExistsCriteria() {
    	return false;
    }
    
    @Override
    public boolean supportsInCriteriaSubquery() {
    	return false;
    }
    
    @Override
    public boolean supportsInlineViews() {
    	return false;
    }
    
    @Override
    public boolean supportsOrderByNullOrdering() {
    	return false;
    }
    
    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
    	return false;
    }
    
    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
    	return false;
    }
    
    @Override
    public boolean supportsScalarSubqueries() {
    	return false;
    }
    
    @Override
    public boolean supportsSearchedCaseExpressions() {
    	return false;
    }
    
    @Override
    public boolean supportsExcept() {
    	return true;
    }
    
    @Override
    public boolean supportsIntersect() {
    	return true;
    }
    
    @Override
    public boolean supportsSetQueryOrderBy() {
    	return false;
    }
        
}
