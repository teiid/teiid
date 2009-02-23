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
package com.metamatrix.connector.jdbc.sybase;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.SourceSystemFunctions;
import com.metamatrix.connector.jdbc.translator.AliasModifier;
import com.metamatrix.connector.jdbc.translator.SubstringFunctionModifier;
import com.metamatrix.connector.jdbc.translator.Translator;
import com.metamatrix.connector.language.ILimit;

/**
 */
public class SybaseSQLTranslator extends Translator {
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#initialize(com.metamatrix.data.api.ConnectorEnvironment, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier(getLanguageFactory())); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("+")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.LENGTH, new AliasModifier(getLengthFunctionName())); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("isnull")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new SubstringFunctionModifier(getLanguageFactory(), "substring", getLengthFunctionName())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new SybaseConvertModifier(getLanguageFactory()));      //$NON-NLS-1$   
    }
    
    public String getLengthFunctionName() {
    	return "char_length"; //$NON-NLS-1$
    }
    
    @Override
    public boolean useAsInGroupAlias() {
    	return false;
    }
    
    @Override
    public boolean hasTimeType() {
    	return false;
    }
    
    @Override
    public String addLimitString(String queryCommand, ILimit limit) {
    	return "SELECT TOP " + limit.getRowLimit() + " * FROM (" + queryCommand + ") AS X"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
}
