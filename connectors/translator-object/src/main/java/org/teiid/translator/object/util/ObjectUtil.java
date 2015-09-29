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
package org.teiid.translator.object.util;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.translator.TranslatorException;

/**
 * @author vanhalbert
 *
 */
public class ObjectUtil {

	public static Object convertValueToObjectType(Object value, Column mdIDElement) throws TranslatorException {
		if (value instanceof String && mdIDElement.getJavaType().getName().equals(String.class.getName())) {
			return escapeReservedChars(value);
		}
		try {
			value  = DataTypeManager.transformValue(value,  mdIDElement.getJavaType());
			return value;
		} catch (TransformationException e) {
			throw new TranslatorException(e);
		}
	}
	
	private static Object escapeReservedChars(final Object value) {

		String expr = (String) value;

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < expr.length(); i++) {
			char curChar = expr.charAt(i);
			switch (curChar) {
			case '\\':
				sb.append("\\5c"); //$NON-NLS-1$
				break;
			case '*':
				sb.append("\\2a"); //$NON-NLS-1$
				break;
			case '(':
				sb.append("\\28"); //$NON-NLS-1$
				break;
			case ')':
				sb.append("\\29"); //$NON-NLS-1$
				break;
			case '\u0000':
				sb.append("\\00"); //$NON-NLS-1$
				break;
			default:
				sb.append(curChar);
			}
		}
		return sb.toString();
	}
	
	public static String getRecordName(Column col) {
		return SQLStringVisitor.getRecordName(col);
	}
	
	public static String getRecordName(ForeignKey c) {
		String name = c.getNameInSource();
		if (name == null || name.trim().isEmpty()) {
			return c.getName();
		}
		return name;
	}
}
