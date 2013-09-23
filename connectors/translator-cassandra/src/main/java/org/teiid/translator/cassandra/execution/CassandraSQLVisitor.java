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

package org.teiid.translator.cassandra.execution;

import static org.teiid.language.SQLConstants.Reserved.FROM;
import static org.teiid.language.SQLConstants.Reserved.SELECT;
import static org.teiid.language.SQLConstants.Reserved.WHERE;

import org.teiid.language.LanguageObject;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;

public class CassandraSQLVisitor extends SQLStringVisitor {

	public String getTranslatedSQL() {
		return buffer.toString();
	}
	
	@Override
	protected String replaceElementName(String group, String element) {
		return element;
	}

	public void translateSQL(LanguageObject obj) {
		append(obj);
	}

	@Override
	public void visit(Select obj) {
		buffer.append(SELECT).append(Tokens.SPACE);
		if (obj.getFrom() != null && !obj.getFrom().isEmpty()){
			NamedTable table = (NamedTable)obj.getFrom().get(0);
		
			if(table.getMetadataObject().getColumns() !=  null){
				if (obj.getDerivedColumns().size() == table.getMetadataObject().getColumns().size()){
					buffer.append("*");
				}else{
					append(obj.getDerivedColumns());
				}
			}
			buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);
			append(obj.getFrom());
		}


		if(obj.getWhere() != null){
			buffer.append(Tokens.SPACE).append(WHERE).append(Tokens.SPACE);
			append(obj.getWhere());
		}
		
		if(obj.getOrderBy() != null){
			buffer.append(Tokens.SPACE);
			append(obj.getOrderBy());
		}
		
		if(obj.getLimit() != null){
			buffer.append(Tokens.SPACE);
			append(obj.getLimit());
		}
	}
}
