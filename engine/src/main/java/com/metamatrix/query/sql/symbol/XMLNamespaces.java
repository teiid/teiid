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

package com.metamatrix.query.sql.symbol;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

public class XMLNamespaces implements LanguageObject {
	
	private static final long serialVersionUID = 681076404921001047L;

	public static class NamespaceItem {
		private String uri;
		private String prefix;
		
		public NamespaceItem(String uri, String prefix) {
			this.uri = uri;
			this.prefix = prefix;
		}
		
		public NamespaceItem(String defaultNamepace) {
			this.uri = defaultNamepace;
		}
		
		public NamespaceItem() {
		}
		
		public String getUri() {
			return uri;
		}
		
		public String getPrefix() {
			return prefix;
		}
		
		@Override
		public int hashCode() {
			return HashCodeUtil.hashCode(0, this.uri, this.prefix);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof NamespaceItem)) {
				return false;
			}
			NamespaceItem other = (NamespaceItem)obj;
			return EquivalenceUtil.areEqual(this.uri, other.uri) &&
				EquivalenceUtil.areEqual(this.prefix, other.prefix);
		}
	}

	private List<NamespaceItem> namespaceItems;
	
	
	public XMLNamespaces(List<NamespaceItem> namespaceItems) {
		this.namespaceItems = namespaceItems;
	}
	
	public List<NamespaceItem> getNamespaceItems() {
		return namespaceItems;
	}
	
	@Override
	public XMLNamespaces clone() {
		XMLNamespaces clone = new XMLNamespaces(new ArrayList<NamespaceItem>(namespaceItems));
		return clone;
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(namespaceItems.hashCode());
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof XMLNamespaces)) {
			return false;
		}
		XMLNamespaces other = (XMLNamespaces)obj;
		return namespaceItems.equals(other.namespaceItems);
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
