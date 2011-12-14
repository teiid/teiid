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

package org.teiid.translator.jdbc.mysql;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Function;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;

@Translator(name="mysql5", description="A translator for open source MySQL5 Database")
public class MySQL5ExecutionFactory extends MySQLExecutionFactory {
	
	@Override
    public void start() throws TranslatorException {
        super.start();
        registerFunctionModifier(SourceSystemFunctions.CHAR, new FunctionModifier() {
			
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("char(", function.getParameters().get(0), " USING ASCII)"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
	}
	
	@Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPADD);
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPDIFF);
        return supportedFunctions;
    }
    
    @Override
    public boolean supportsInlineViews() {
    	return true;
    }
    
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return true;
    }
    
    @Override
    public boolean supportsLikeRegex() {
    	return true;
    }
    
    @Override
    public String getLikeRegexString() {
    	return "REGEXP"; //$NON-NLS-1$
    }
    
    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
    		Class<?> expectedType) throws SQLException {
    	Object result = super.retrieveValue(results, columnIndex, expectedType);
    	if (expectedType == TypeFacility.RUNTIME_TYPES.STRING && result instanceof Blob) {
    		return results.getString(columnIndex);
    	}
    	return result;
    }
    
}
