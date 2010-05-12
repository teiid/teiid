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
package org.teiid.translator.jdbc.sqlserver;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.translator.ConnectorCapabilities;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.sybase.SybaseSQLTranslator;

/**
 * Updated to assume the use of the DataDirect, 2005 driver, or later.
 */
public class SQLServerSQLTranslator extends SybaseSQLTranslator {
	
	//TEIID-31 remove mod modifier for SQL Server 2008
	
	@Override
	protected List<Object> convertDateToString(Function function) {
		return Arrays.asList("replace(convert(varchar, ", function.getParameters().get(0), ", 102), '.', '-')"); //$NON-NLS-1$ //$NON-NLS-2$
	}
    
	@Override
	protected List<?> convertTimestampToString(Function function) {
		return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ", 21)"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return SQLServerCapabilities.class;
    }
    
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (obj instanceof ColumnReference) {
    		ColumnReference elem = (ColumnReference)obj;
			if (TypeFacility.RUNTIME_TYPES.STRING.equals(elem.getType()) && elem.getMetadataObject() != null && "uniqueidentifier".equalsIgnoreCase(elem.getMetadataObject().getNativeType())) { //$NON-NLS-1$
				return Arrays.asList("cast(", elem, " as char(36))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
    	}
    	return super.translate(obj, context);
    }
    
}
