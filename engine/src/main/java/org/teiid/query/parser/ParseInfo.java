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

package org.teiid.query.parser;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.teiid.query.sql.lang.CacheHint;


public class ParseInfo implements Serializable{

	private static final long serialVersionUID = -7323683731955992888L;

	public Map<String, Integer> nameCounts = new HashMap<String, Integer>();
	
    public int referenceCount = 0;

    // treat a double quoted variable as variable instead of string 
    public boolean ansiQuotedIdentifiers=Boolean.valueOf(System.getProperty("org.teiid.ansiQuotedIdentifiers", Boolean.TRUE.toString())).booleanValue(); //$NON-NLS-1$
    
    public CacheHint cacheHint;
    
	public ParseInfo() { }
	
	public boolean useAnsiQuotedIdentifiers() {
	    return ansiQuotedIdentifiers;
	}
	
	@Override
	public int hashCode() {
		return ansiQuotedIdentifiers?1:0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof ParseInfo)) {
			return false;
		}
		ParseInfo other = (ParseInfo)obj;
		return this.ansiQuotedIdentifiers == other.ansiQuotedIdentifiers;
	}
}