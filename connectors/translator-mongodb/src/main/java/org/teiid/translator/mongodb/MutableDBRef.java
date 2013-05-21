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
package org.teiid.translator.mongodb;

import com.mongodb.DB;
import com.mongodb.DBRef;

public class MutableDBRef {
	enum Assosiation {ONE, MANY};

	private String parentTable;
	private Object id;
	private String referenceColumnName;
	private String columnName;

	private String embeddedTable;
	private Assosiation assosiation;

	public DBRef getDBRef(DB db, boolean push) {
		return new DBRef(db, push?this.parentTable:this.embeddedTable, this.id);
	}

	public String getParentTable() {
		return this.parentTable;
	}

	public void setParentTable(String parentTable) {
		this.parentTable = parentTable;
	}

	public Object getId() {
		return this.id;
	}

	public void setId(Object id) {
		this.id = id;
	}

	public String getReferenceColumnName() {
		return this.referenceColumnName;
	}

	public void setReferenceColumnName(String columnName) {
		this.referenceColumnName = columnName;
	}

	public String getEmbeddedTable() {
		return this.embeddedTable;
	}

	public void setEmbeddedTable(String embeddedTable) {
		this.embeddedTable = embeddedTable;
	}

	public Assosiation getAssosiation() {
		return this.assosiation;
	}

	public void setAssosiation(Assosiation assosiation) {
		this.assosiation = assosiation;
	}

	public String getColumnName() {
		return this.columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ParentTable:").append(this.parentTable); //$NON-NLS-1$
		sb.append(" id:").append(this.id); //$NON-NLS-1$
		sb.append(" EmbeddedTable:").append(this.embeddedTable); //$NON-NLS-1$
		return sb.toString();
	}
}
