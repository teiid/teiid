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



package com.metamatrix.connector.xml.base;

import java.text.MessageFormat;
import java.util.ArrayList;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.metadata.runtime.Element;

public class ValueConvertor {

	public static Object convertString(String value, Class type,
			ConnectorEnvironment env, ExecutionContext ctx)
			throws ConnectorException {

		String newValue = value;
		if (newValue == null) {
			return null;
		}

		if (type == TypeFacility.RUNTIME_TYPES.STRING) {
			return newValue;
		}

		// What is this for??
		if (type != java.lang.Character.class) {
			newValue = newValue.replaceAll(",", ""); //$NON-NLS-1$ //$NON-NLS-2$
		}
		return attemptTransformation(newValue, type, env);
	}

	private static Object attemptTransformation(String value, Class type, ConnectorEnvironment env) throws ConnectorException {
		TypeFacility typeFacility = env.getTypeFacility();
		
		String newValue = value;
		if (typeFacility.hasTransformation(String.class, type)) {
			return typeFacility.transformValue(newValue, String.class, type);
		} 
		else {
			String msgRaw = Messages.getString("XMLExecutionImpl.type.conversion.failure"); //$NON-NLS-1$
			String msg = MessageFormat.format(msgRaw, new Object[] { newValue,type });
			throw new ConnectorException(msg);
		}
	}

	public static boolean compareData(LargeOrSmallString dataStr,
			String compareStr, Element elementMetadata,
			ConnectorEnvironment env, ExecutionContext ctx)
			throws ConnectorException {
		if (dataStr == null) {
			return false; // we are not testing for nulls, if compared to
			// anything else, it fails
		}
		// This will not well if the data being compared to is very large,
		// but comparisons of that sort are unusual. Still, it needs to be
		// rectified.
		String dataValueStr = dataStr.getAsString();

		String compareValueStr = new String(compareStr);
		if (!elementMetadata.isCaseSensitive()) {
			dataValueStr = dataValueStr.toUpperCase();
			compareValueStr = compareValueStr.toUpperCase();
		}
		Class dataValueType = elementMetadata.getJavaType();
		// Convert String to appropriate data type
		Object value = convertString(dataValueStr, dataValueType, env, ctx);

		// String to compare from criteria
		Object compareValue = convertString(compareValueStr, dataValueType,
				env, ctx);

		// null != null, so if either is null, do not match
		// if (value == null || compareValue == null) {
		// return false;
		// }

		// don't Check that both objects are of same type (they should be)
		// if (!dataValueType.equals(compareValue.getClass())) {
		// String msgRaw =
		// Messages.getString("XMLExecutionImpl.invalid.comparison");
		// String msg = MessageFormat.format(msgRaw, new Object[] {dataValueStr,
		// compareValueStr});
		// throw new ConnectorException(msg);
		// }

		// Do compareTo
		int comparison = 0;
		Comparable comparable = (Comparable) value;
		comparison = comparable.compareTo(compareValue);
		return comparison == 0;
	}
	
    public static boolean evaluate(LargeOrSmallString currentValue,
            CriteriaDesc criteria, ConnectorEnvironment env, ExecutionContext ctx) throws ConnectorException {
        // this is the criteriaq for the col
        ArrayList values = criteria.getValues();
        Element element = criteria.getElement();
        for (int y = 0; y < values.size(); y++) {
            if (ValueConvertor.compareData(currentValue, (String) values.get(y), element, env, ctx)) {
                return true;
            }
        }
        return false; // no matching value

    }
    
    public static Object convertLargeOrSmallString(LargeOrSmallString value,
            Class type, ConnectorEnvironment env, ExecutionContext ctx) throws ConnectorException {
        if (value == null) {
            return null;
        }
        return ValueConvertor.convertString(value.getAsString(), type, env, ctx);
    }
}
