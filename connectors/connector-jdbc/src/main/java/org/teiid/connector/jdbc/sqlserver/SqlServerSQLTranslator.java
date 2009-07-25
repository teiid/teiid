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
package org.teiid.connector.jdbc.sqlserver;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.jdbc.sybase.SybaseSQLTranslator;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.jdbc.translator.EscapeSyntaxModifier;

/**
 * Updated to assume the use of the DataDirect, 2005 driver, or later.
 */
public class SqlServerSQLTranslator extends SybaseSQLTranslator {

    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
        //TEIID-31 remove mod modifier for SQL Server 2008
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.REPEAT, new AliasModifier("replicate")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.HOUR, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.SECOND, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.WEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier());
    }
    
    @Override
    public String getLengthFunctionName() {
    	return "len"; //$NON-NLS-1$
    }
    
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return SqlServerCapabilities.class;
    }
    
}
