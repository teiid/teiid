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

package org.teiid.translator.cassandra;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Date;
import java.util.UUID;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.translator.TypeFacility;

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
				append(obj.getDerivedColumns());
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
	
	@Override
	public void visit(Literal obj) {
		if (obj.getValue() == null) {
			super.visit(obj);
			return;
		}
		if (obj.getValue() instanceof Date) {
			buffer.append(((Date)obj.getValue()).getTime());
			return;
		}
		//cassandra directly parses uuids
		if (obj.getValue() instanceof UUID) {
			buffer.append(obj.getValue());
			return;
		}
		//TODO: only supported with Cassandra 2 or later
		/*if (obj.isBindEligible() 
				|| obj.getType() == TypeFacility.RUNTIME_TYPES.OBJECT
				|| type == TypeFacility.RUNTIME_TYPES.VARBINARY) {
			if (values == null) {
				values = new ArrayList<Object>();
			}
			buffer.append('?');
			if (type == TypeFacility.RUNTIME_TYPES.VARBINARY) {
				values.add(ByteBuffer.wrap(((BinaryType)obj.getValue()).getBytesDirect()));
			} else {
				values.add(obj.getValue());
			}
			return;
		}*/
		Class<?> type = obj.getType();
		if (type == TypeFacility.RUNTIME_TYPES.VARBINARY) {
			buffer.append("0x") //$NON-NLS-1$
			  .append(obj.getValue());
			return;
		}
		if (type == TypeFacility.RUNTIME_TYPES.BLOB) {
			buffer.append("0x"); //$NON-NLS-1$
			Blob b = (Blob)obj.getValue();
			InputStream binaryStream = null;
			try {
				if (b.length() > Integer.MAX_VALUE) {
					throw new AssertionError("Blob is too large"); //$NON-NLS-1$
				}
				binaryStream = b.getBinaryStream();
				PropertiesUtils.toHex(buffer, binaryStream);
			} catch (SQLException e) {
				throw new TeiidRuntimeException(e);
			} catch (IOException e) {
				throw new TeiidRuntimeException(e);
			} finally {
				if (binaryStream != null) {
					try {
						binaryStream.close();
					} catch (IOException e) {
					}
				}
			}
			return;
		}
		if (!Number.class.isAssignableFrom(type) 
				&& type != TypeFacility.RUNTIME_TYPES.BOOLEAN 
				&& type != TypeFacility.RUNTIME_TYPES.VARBINARY) {
			//just handle as strings things like timestamp
			type = TypeFacility.RUNTIME_TYPES.STRING;
		}
		super.appendLiteral(obj, type);
	}
}
