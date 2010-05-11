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

package org.teiid.query.optimizer;

import java.util.List;
import java.util.Set;

public class InlineViewCase {
	public String name;
	public String userQuery;
	public String optimizedQuery;
	public Set<String> sourceQueries;
	public List<List<Object>> expectedResults;
	
	public String getFullyQualifiedQuery() {
		return optimizedQuery;    	
	}		
	public InlineViewCase(String name, String userQuery, String optimizedQuery, Set<String> sourceQueries, List expectedResults) {
		this.name = name;
		this.userQuery = userQuery;
		this.optimizedQuery = optimizedQuery;
		this.sourceQueries = sourceQueries;
		this.expectedResults = expectedResults;
	}	
}
