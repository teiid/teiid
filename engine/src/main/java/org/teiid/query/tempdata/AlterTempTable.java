/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.tempdata;

import java.util.List;
import java.util.Set;

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;

public class AlterTempTable extends Command {
	
	private String tempTable;
	private Set<List<ElementSymbol>> indexColumns;
	
	public AlterTempTable(String tempTable) {
		this.tempTable = tempTable;
	}
	
	public String getTempTable() {
		return tempTable;
	}
	
	public Set<List<ElementSymbol>> getIndexColumns() {
		return indexColumns;
	}
	
	public void setIndexColumns(Set<List<ElementSymbol>> indexColumns) {
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
	public List<Expression> getProjectedSymbols() {
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
