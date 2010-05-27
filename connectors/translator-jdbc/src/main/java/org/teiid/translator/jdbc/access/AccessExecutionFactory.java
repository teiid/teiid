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

import java.util.List;

import org.teiid.translator.Translator;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.sybase.SybaseExecutionFactory;

@Translator(name="access")
public class AccessExecutionFactory extends SybaseExecutionFactory {
	
	public AccessExecutionFactory() {
		setSupportsOrderBy(false);
	}
	
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "-1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
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
    public boolean supportsInsertWithQueryExpression() {
    	return false;
    }
    
    @Override
    public boolean supportsInlineViews() {
        return false;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return false;
    }
    
    @Override
    public int getMaxInCriteriaSize() {
    	return JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA;
    }
    
    @Override
    public int getMaxFromGroups() {
        return DEFAULT_MAX_FROM_GROUPS;
    } 
    
    @Override
    public List<String> getSupportedFunctions() {
    	return getDefaultSupportedFunctions();
    }
}
