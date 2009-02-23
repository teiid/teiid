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

package com.metamatrix.connector.jdbc.derby;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.SourceSystemFunctions;
import com.metamatrix.connector.jdbc.translator.AliasModifier;
import com.metamatrix.connector.jdbc.translator.EscapeSyntaxModifier;
import com.metamatrix.connector.jdbc.translator.Translator;


/** 
 * @since 4.3
 */
public class DerbySQLTranslator extends Translator {

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new EscapeSyntaxModifier()); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier()); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier()); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new DerbyConvertModifier(getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.COALESCE, new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$        
    }  
 
    @Override
    public boolean addSourceComment() {
        return false;
    }
    
    @Override
    public String getDefaultConnectionTestQuery() {
    	return "Select 0 from sys.systables where 1 = 2"; //$NON-NLS-1$
    }

}
