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

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.jdbc.sybase.SybaseSQLTranslator;
import org.teiid.connector.language.IFunction;

/**
 * Updated to assume the use of the DataDirect, 2005 driver, or later.
 */
public class SqlServerSQLTranslator extends SybaseSQLTranslator {
	
	//TEIID-31 remove mod modifier for SQL Server 2008
	
	@Override
	protected List<Object> convertDateToString(IFunction function) {
		return Arrays.asList("replace(convert(varchar, ", function.getParameters().get(0), ", 102), '.', '-')"); //$NON-NLS-1$ //$NON-NLS-2$
	}
    
	@Override
	protected List<?> convertTimestampToString(IFunction function) {
		return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ", 21)"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return SqlServerCapabilities.class;
    }
    
}
