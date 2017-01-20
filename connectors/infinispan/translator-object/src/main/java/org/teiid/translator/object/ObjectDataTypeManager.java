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

import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.translator.TypeFacility;

/**
 * The ObjectDataTypeManager provides the control from converting from and to the source data type.
 * The {#link ClassRegistry} is the keeper of the data type manager and that is where a specific implementation
 * can be controlled when the data source has specific object type conversion considerations.
 * 
 * @author vhalbert
 *
 */
public class ObjectDataTypeManager {

	public String getDataTypeName(Class<?> type)  {
		return TypeFacility.getDataTypeName(TypeFacility.getRuntimeType(type));
	}
	
	public String getNativeTypeName(Class<?> clzz) {

		if (clzz.isArray()) {
			return clzz.getSimpleName();
		} else if (clzz.isEnum()) {
			return Enum.class.getName();
		} else {
			return clzz.getName();
		}
	}
	
	public Object convertToObjectType(Object value, Class<?> objectType) throws TransformationException  {

		if (objectType.isEnum()) {
			Object[] con = objectType.getEnumConstants();
			for (Object c:con) {
				if (c.toString().equalsIgnoreCase(value.toString())) {
					return c;
				}
			}
		}
		
		if (value.getClass().isAssignableFrom(objectType)) {
			return value;
		}
		
		if (value instanceof BinaryType) {
			BinaryType bt = (BinaryType) value;
			return bt.getBytes();	
		}
		
		if (DataTypeManager.isTransformable(value.getClass(), objectType)) {
			return DataTypeManager.transformValue(value,  objectType ); //column.getJavaType()
		}
		
		if (objectType.isInstance(value)) {
			return value;
		}
		
		return null;
	}
	
	public  Object convertFromObjectType(Object value, String teiidType) throws TransformationException  {
		if (value == null) {
			return null;
		}
		
		Class<?> clz = DataTypeManager.getDataTypeClass(teiidType);
		
		return convertFromObjectType(value, clz);
	}
	
	public  Object convertFromObjectType(Object value, Class<?> teiidType) throws TransformationException  {
		if (value == null) {
			return null;
		}
		
		if (value.getClass().isArray() || 
				teiidType.isInstance(value) ||
				teiidType.getClass().getName().equals(java.lang.Object.class.getName()
						
			) ) {
			return value;
			
		}

		
		if (DataTypeManager.isTransformable(value.getClass(), teiidType)) {
			return DataTypeManager.transformValue(value, teiidType);
		}
		
//		if (value instanceof LocalDateTime) {
//			return new Timestamp(((LocalDateTime)value).toDateTime().getMillis());
//		} else if (value instanceof Guid) {
//			return value.toString();
//		} else if (value instanceof LocalTime) {
//			return new Time(((LocalTime)value).toDateTimeToday().getMillis());
//		}
		return value;
	}
}
