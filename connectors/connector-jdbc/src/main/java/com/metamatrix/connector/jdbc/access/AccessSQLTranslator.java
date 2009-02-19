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
package com.metamatrix.connector.jdbc.access;

import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.connector.language.ILimit;

public class AccessSQLTranslator extends SQLTranslator {
	
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
    public String addLimitString(String queryCommand, ILimit limit) {
    	int index = queryCommand.startsWith("SELECT DISTINCT")?15:6;
    	return new StringBuffer(queryCommand.length() + 8).append(queryCommand)
				.insert(index, " TOP " + limit.getRowLimit()).toString();
    }
    
    @Override
    public boolean addSourceComment() {
    	return false;
    }
    
}
