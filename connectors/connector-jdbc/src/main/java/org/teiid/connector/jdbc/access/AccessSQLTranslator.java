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
package org.teiid.connector.jdbc.access;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.IQueryCommand;


public class AccessSQLTranslator extends Translator {
	
	@Override
	public boolean hasTimeType() {
		return false;
	}

    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "-1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
    }
    
    @Override
    public List<?> translateCommand(ICommand command, ExecutionContext context) {
    	if (!(command instanceof IQueryCommand)) {
    		return null;
    	}
		IQueryCommand queryCommand = (IQueryCommand)command;
		if (queryCommand.getLimit() == null) {
			return null;
    	}
		ILimit limit = queryCommand.getLimit();
		IOrderBy orderBy = queryCommand.getOrderBy();
		queryCommand.setLimit(null);
		queryCommand.setOrderBy(null);
		List<Object> parts = new ArrayList<Object>(6);
		parts.add("SELECT TOP ");
		parts.add(limit.getRowLimit());
		parts.add(" * FROM (");
		parts.add(queryCommand);
		parts.add(") AS X");
		if (orderBy != null) {
			parts.add(orderBy);
		}
		return parts;
    }
                
    @Override
    public boolean addSourceComment() {
    	return false;
    }
    
}
