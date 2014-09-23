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
package org.teiid.olingo;

import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceAction;
import org.apache.olingo.server.api.uri.UriResourceComplexProperty;
import org.apache.olingo.server.api.uri.UriResourceCount;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceFunction;
import org.apache.olingo.server.api.uri.UriResourceIt;
import org.apache.olingo.server.api.uri.UriResourceLambdaAll;
import org.apache.olingo.server.api.uri.UriResourceLambdaAny;
import org.apache.olingo.server.api.uri.UriResourceLambdaVariable;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceRef;
import org.apache.olingo.server.api.uri.UriResourceRoot;
import org.apache.olingo.server.api.uri.UriResourceSingleton;
import org.apache.olingo.server.api.uri.UriResourceValue;

public class ResourcePropertyCollector extends
		DefaultODataResourceURLHierarchyVisitor {
	
	private UriResource resource;
	private boolean isCount;
	
	public static UriResource getUriResource(UriInfoResource uriInfo) {
		ResourcePropertyCollector visitor = new ResourcePropertyCollector();
		visitor.visit(uriInfo);
		return visitor.resource;
	}
	
	public UriResource getResource() {
		return resource;
	}
	
	public boolean isCount() {
		return isCount;
	}
	
	@Override
	public void visit(UriResourceComplexProperty info) {
		this.resource = info;
	}

	@Override
	public void visit(UriResourcePrimitiveProperty info) {
		this.resource = info;
	}	
	
	@Override
	public void visit(UriResourceNavigation info) {
		this.resource = info;
	}	
	
	@Override
	public void visit(UriResourceCount option) {
		this.isCount = true;
	}
	
	public void visit(UriResourceAction resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceEntitySet resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceFunction resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$		
	}
	public void visit(UriResourceIt resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceLambdaAll resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceLambdaAny resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceLambdaVariable resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceRef resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceRoot resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void visit(UriResourceSingleton resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
	public void  visit(UriResourceValue resource) {
		throw new IllegalArgumentException("not supported"); //$NON-NLS-1$
	}
}
