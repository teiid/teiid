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

package org.teiid.metadata;

import org.teiid.CommandContext;

/**
 * A hook for providing {@link ViewDefinition}s
 */
public interface MetadataProvider {
	
	public enum Scope {
		/**
		 * The {@link ViewDefinition} applies only to the calling user 
		 */
		USER,
		/**
		 * The {@link ViewDefinition} applies to all users
		 */
		VDB
	}
	
	public static class ViewDefinition {
		private String sql;
		private Scope scope = Scope.VDB;
		
		public ViewDefinition(String sql, Scope scope) {
			this.sql = sql;
			this.scope = scope;
		}

		public String getSql() {
			return sql;
		}
		
		public Scope getScope() {
			return scope;
		}
	}
		
	/**
	 * Returns an updated {@link ViewDefinition} or null if the default view definition 
	 * should be used.
	 * @param viewName
	 * @param context
	 * @return
	 */
	ViewDefinition getViewDefinition(String schema, String viewName, CommandContext context);
	
}
