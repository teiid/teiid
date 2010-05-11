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

package org.teiid.core.types;

import org.teiid.core.CorePlugin;

/**
 * This interface represents the transformation from one data type to
 * another.  For instance, from java.lang.String to java.lang.Integer
 * where java.lang.String is the the source type, "java.lang.String"
 * is the source name, etc.
 */
public abstract class Transform {
	
	/**
	 * This method transforms a value of the source type into a value
	 * of the target type.
	 * @param value Incoming value of source type
	 * @return Outgoing value of target type
	 * @throws TransformationException if value is an incorrect input type or
	 * the transformation fails
	 */
	public Object transform(Object value) throws TransformationException {
		if (value == null) {
			return null;
		}
		return transformDirect(value);
	}
	
	
	protected abstract Object transformDirect(Object value) throws TransformationException;

	/**
	 * Type of the incoming value.
	 * @return Source type
	 */
	public abstract Class<?> getSourceType();

	/**
	 * Name of the source type.
	 * @return Name of source type
	 */
	public String getSourceTypeName() {
		return DataTypeManager.getDataTypeName(getSourceType());
	}

	/**
	 * Type of the outgoing value.
	 * @return Target type
	 */
	public abstract Class<?> getTargetType();

	/**
	 * Name of the target type.
	 * @return Name of target type
	 */
	public String getTargetTypeName() {
        return DataTypeManager.getDataTypeName(getTargetType());
	}

	/**
	 * Get nice display name for GUIs.
	 * @return Display name
	 */
	public String getDisplayName() {
		return getSourceTypeName() + " to " + getTargetTypeName(); //$NON-NLS-1$
	}

	/**
	 * Get description.
	 * @return Description of transform
	 */
	public String getDescription() {
		return getDisplayName();
	}

	public boolean isExplicit() {
		return false;
	}
	
	/**
	 * Override Object.toString() to do getDisplayName() version.
	 * @return String representation of object
	 */
	public String toString() {
		return getDisplayName();
	}

	protected void checkValueRange(Object value, Number min, Number max)
			throws TransformationException {
		if (((Comparable)value).compareTo(DataTypeManager.transformValue(min, getSourceType())) < 0 || ((Comparable)value).compareTo(DataTypeManager.transformValue(max, getSourceType())) > 0) {
			throw new TransformationException(CorePlugin.Util.getString("transform.value_out_of_range", value, getSourceType().getSimpleName(), getTargetType().getSimpleName())); //$NON-NLS-1$
		}
	}

}
