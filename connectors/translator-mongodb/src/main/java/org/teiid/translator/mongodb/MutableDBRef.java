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

import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.TranslatorException;

import com.mongodb.DB;
import com.mongodb.DBRef;

public class MutableDBRef implements Cloneable {
	enum Assosiation {ONE, MANY};

	private String parentTable;
	private IDRef id;
	private List<String> referenceColumns;
	private List<String> columns;

	private String embeddedTable;
	private Assosiation assosiation;
	private String name;
	private String idReference;
	private String referenceName;

	public String getReferenceName() {
		// this is name of the reference key in the document that the embedded document represents.
		return this.referenceName;
	}

	public void setReferenceName(String name) {
		this.referenceName = name;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DBRef getDBRef(DB db, boolean push) {
		if (this.id != null) {
			if (this.idReference != null) {
				return new DBRef(db, push?this.parentTable:this.embeddedTable, new DBRef(db, this.idReference, this.id.getValue()));
			}
			return new DBRef(db, push?this.parentTable:this.embeddedTable, this.id.getValue());
		}
		return null;
	}

	public String getParentTable() {
		return this.parentTable;
	}

	public void setParentTable(String parentTable) {
		this.parentTable = parentTable;
	}

	public Object getId() throws TranslatorException {
		if (this.id == null) {
			return null;
		}
		if (this.id.pk.keySet().size() != this.columns.size()) {
			throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18014));
		}
		return this.id.getValue();
	}

	public void setId(String column, Object value) {
		if (this.id == null) {
			this.id = new IDRef();
		}
		this.id.addColumn(column, value);
	}

	public List<String> getReferenceColumns() {
		return this.referenceColumns;
	}

	public void setReferenceColumns(List<String> columns) {
		this.referenceColumns = new ArrayList<String>(columns);
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

	public List<String> getColumns() {
		return this.columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = new ArrayList<String>(columns);
	}

	public String getIdReference() {
		return this.idReference;
	}

	public void setIdReference(String idReference) {
		this.idReference = idReference;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ParentTable:").append(this.parentTable); //$NON-NLS-1$
		sb.append(" id:").append(this.id); //$NON-NLS-1$
		sb.append(" EmbeddedTable:").append(this.embeddedTable); //$NON-NLS-1$
		return sb.toString();
	}

	@Override
	public MutableDBRef clone() {
		MutableDBRef clone = new MutableDBRef();
		clone.parentTable = this.parentTable;
		if (this.id != null) {
			clone.id = this.id.clone();
		}
		clone.referenceColumns = new ArrayList(this.referenceColumns);
		clone.columns = new ArrayList<String>(this.columns);
		clone.embeddedTable = this.embeddedTable;
		clone.assosiation = this.assosiation;
		clone.name = this.name;
		clone.idReference = this.idReference;
		clone.referenceName = this.referenceName;
		return clone;
	}
}
