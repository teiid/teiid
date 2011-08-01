package org.teiid.query.tempdata;

import java.util.List;

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;

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

public class AlterTempTable extends Command {
	
	private String tempTable;
	private List<ElementSymbol> indexColumns;
	
	public AlterTempTable(String tempTable) {
		this.tempTable = tempTable;
	}
	
	public String getTempTable() {
		return tempTable;
	}
	
	public List<ElementSymbol> getIndexColumns() {
		return indexColumns;
	}
	
	public void setIndexColumns(List<ElementSymbol> indexColumns) {
		this.indexColumns = indexColumns;
	}
	
	@Override
	public boolean areResultsCachable() {
		return false;
	}

	@Override
	public Object clone() {
		return this;
	}

	@Override
	public List<SingleElementSymbol> getProjectedSymbols() {
		return Command.getUpdateCommandSymbol();
	}

	@Override
	public int getType() {
		return Command.TYPE_UNKNOWN;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		
	}

}
