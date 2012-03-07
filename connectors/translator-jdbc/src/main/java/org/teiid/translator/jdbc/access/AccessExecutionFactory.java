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

/*
 */
package org.teiid.translator.jdbc.access;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.AggregateFunction;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.sybase.BaseSybaseExecutionFactory;

@Translator(name="access", description="A translator for Microsoft Access Database")
public class AccessExecutionFactory extends BaseSybaseExecutionFactory {
	
	public AccessExecutionFactory() {
		setSupportsOrderBy(false);
		setDatabaseVersion("2003"); //$NON-NLS-1$
		setMaxInCriteriaSize(JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA);
		setMaxDependentInPredicates(10); //sql length length is 64k
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		registerFunctionModifier(SourceSystemFunctions.ASCII, new AliasModifier("Asc")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("Chr")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CONCAT, new FunctionModifier() {
			
			@Override
			public List<?> translate(Function function) {
				List<Object> result = new ArrayList<Object>(function.getParameters().size()*2 - 1);
				for (int i = 0; i < function.getParameters().size(); i++) {
					if (i > 0) {
						result.add(" & "); //$NON-NLS-1$
					}
					result.add(function.getParameters().get(i));
				}
				return result;
			}
		});
		registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier("Len")); //$NON-NLS-1$
	}
	
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "-1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
    }
    
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (obj instanceof AggregateFunction) {
    		AggregateFunction af = (AggregateFunction)obj;
    		if (af.getName().equals(AggregateFunction.STDDEV_POP)) {
    			af.setName("StDevP"); //$NON-NLS-1$
    		} else if (af.getName().equals(AggregateFunction.STDDEV_SAMP)) {
    			af.setName("StDev"); //$NON-NLS-1$
    		} else if (af.getName().equals(AggregateFunction.VAR_POP)) {
    			af.setName("VarP"); //$NON-NLS-1$
    		} else if (af.getName().equals(AggregateFunction.VAR_SAMP)) {
    			af.setName("Var"); //$NON-NLS-1$
    		}
    	}
    	return super.translate(obj, context);
    }
    
    @Override
    public boolean addSourceComment() {
    	return false;
    }
    
    @Override
    public boolean supportsRowLimit() {
        return true;
    }
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add(SourceSystemFunctions.ABS); 
        supportedFunctions.add(SourceSystemFunctions.EXP); 
        supportedFunctions.add(SourceSystemFunctions.ASCII); 
        supportedFunctions.add(SourceSystemFunctions.CHAR); 
        supportedFunctions.add(SourceSystemFunctions.CONCAT); 
        supportedFunctions.add(SourceSystemFunctions.LCASE); 
        supportedFunctions.add(SourceSystemFunctions.LEFT); 
        supportedFunctions.add(SourceSystemFunctions.LENGTH); 
        supportedFunctions.add(SourceSystemFunctions.LTRIM); 
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RTRIM); 
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        //TODO add support for formatting and use datepart for date methods
        return supportedFunctions;
    }
    
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return true;
    }
}
