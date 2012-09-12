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
package org.teiid.translator.object;

import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.Table;

/**
 * The SelectProjections is responsible for parsing the select attributes from
 * the {@link Command command} into usable information for translating result
 * objects into rows.
 * 
 * @author vhalbert
 * 
 */
public class SelectProjections {

	private boolean isRootTableInSelect = false;

	private Table rootTable = null;

	private String rootClassName = null;

	private SelectProjections(String rootClassName) {
		this.rootClassName = rootClassName;
	}

	public static SelectProjections create(ObjectExecutionFactory factory) {
		return new SelectProjections(factory.getRootClassName());
	}

	public String getRootTableName() {
		return this.rootTable.getName();
	}

	public String getRootNodePrimaryKeyColumnName() {
		if (this.rootTable.getPrimaryKey() != null) {
			return this.rootTable.getPrimaryKey().getColumns().get(0).getName();
		}
		return null;
	}

	public boolean isRootTableInFrom() {
		return this.isRootTableInSelect;
	}

	public void parse(Select query) {
		Table roottable = null;

		List<TableReference> tables = query.getFrom();
		Table lastGroup = null;
		for (TableReference t : tables) {
			if (t instanceof NamedTable) {
				lastGroup = ((NamedTable) t).getMetadataObject();
				if (lastGroup.getNameInSource().equals(rootClassName)) {
					roottable = lastGroup;
					this.isRootTableInSelect = true;
					break;
				}
			}
		}
		// still need to find the roottable, even though its not part of the
		// query
		if (!this.isRootTableInSelect) {
			roottable = determineRootTable(lastGroup);
		}

		this.rootTable = roottable;

	}

	private Table determineRootTable(Table t) {
		if (t == null)
			return null;

		if (t.getForeignKeys() != null && !t.getForeignKeys().isEmpty()) {
			List<ForeignKey> fks = t.getForeignKeys();
			for (ForeignKey fk : fks) {
				if (fk.getPrimaryKey() != null
						&& fk.getPrimaryKey().getParent() != t) {
					return determineRootTable(fk.getPrimaryKey().getParent());
				}
			}
		}

		return t;
	}

	protected void cleanup() {
		rootTable = null;

	}
}
