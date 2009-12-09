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

package org.teiid.dqp.internal.cache;

import java.io.Serializable;
import java.util.List;

import com.metamatrix.core.util.HashCodeUtil;

public class CacheID implements Serializable {
	private String scopeID;
	private String command;
	private int hashCode;
	private List preparedStatementValues;
	
	public CacheID(String scopeID, String command){
		this(scopeID, command, null);
	}
	
	public CacheID(String scopeID, String command, List preparedStatementValue){
		this.scopeID = scopeID;
		this.command = command;
		this.preparedStatementValues = preparedStatementValue;
		hashCode = HashCodeUtil.expHashCode(HashCodeUtil.hashCode(HashCodeUtil.hashCode(0, scopeID), command), preparedStatementValues);
	}

	public String getCommand() {
		return command;
	}
	
	public boolean equals(Object obj){
        if(obj == this) {
            return true;
        } else if(! (obj instanceof CacheID)) {
            return false;
        } else {
        	CacheID that = (CacheID)obj;
            return this.scopeID.equals(that.scopeID)
				&& this.command.equals(that.command)
				&& compareParamValues(preparedStatementValues, that.preparedStatementValues);
		}
	}

	private boolean compareParamValues(List thisPreparedStatementValues, List thatPreparedStatementValues) {
		if(thisPreparedStatementValues == null && thatPreparedStatementValues == null){
			return true;
		}
		if(thisPreparedStatementValues == null || thatPreparedStatementValues == null){
			return false;
		}
		return thisPreparedStatementValues.equals(thatPreparedStatementValues);
	}

	public int hashCode() {
        return hashCode;
    }
	
}
