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

import java.lang.reflect.Method;
import java.util.Map;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.StringUtil;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectPlugin;
import org.teiid.translator.object.ObjectVisitor;
import org.teiid.translator.object.metadata.JavaBeanMetadataProcessor;

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
			value = DataTypeManager.transformValue(value, mdIDElement.getJavaType());
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
		return  SQLStringVisitor.getRecordName(col);	
	}

	public static String getRecordName(ForeignKey c) {
		return SQLStringVisitor.getRecordName(c);
	}
	
	public static String getRecordName(Table table) {	
		return  SQLStringVisitor.getRecordName(table);	
	}

	
	public static Class<?> getRegisteredClass(ClassRegistry classRegistry, ObjectVisitor visitor) throws TranslatorException {
		String tname = (visitor.getPrimaryTable() != null ? visitor.getPrimaryTable() :  visitor.getTableName());

		Class<?> clz = classRegistry.getRegisteredClassUsingTableName(tname);
		if (clz == null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21005, new Object[] {visitor.getTableName()}));
		}
		return clz;
	}

	public static Method findMethod(Map<String, Method> mapMethods, String methodName)  {
		if (methodName == null || methodName.length() == 0) {
			return null;
		}
		
		if (methodName.contains(".")) {
			methodName = StringUtil.getLastToken(methodName, ".");
		}

		return searchMethodMap(mapMethods, methodName);
	}
	
	public static Method searchMethodMap(Map<String, Method> mapMethods, String methodName)  {
		Method m = mapMethods.get(methodName.toLowerCase());
		if (m != null) {
			return m;

		} 
		m = mapMethods.get(methodName);
		if (m != null) {
			return m;
		}
		
		String nm = getNameFromMethodName(methodName);
		if (! methodName.equals(nm)) {
			return findMethod(mapMethods, nm);
		}
		
		return null;

	}
	
	/*
	 * Utility method for stripping a GET, SET or IS prefix from the methodname before comparing
	 */
	public static String getNameFromMethodName(String name) {
		String tolower = name.toLowerCase();
		if (tolower.startsWith(JavaBeanMetadataProcessor.GET) || tolower.startsWith(JavaBeanMetadataProcessor.SET)) {
			return name.substring(3);
		}
		if (tolower.startsWith(JavaBeanMetadataProcessor.IS)) {
			return name.substring(2);
		}
		// return null to indicate no name was found within the name
		return tolower;
	}

}
