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

package com.metamatrix.common.types;

import com.metamatrix.core.util.HashCodeUtil;

/**
 * This fills in most of the details of a Transform and provides additional
 * helpful methods as well.  Transform writers should only need to fill in
 * getSourceType(), getTargetType(), and transform().
 */
public abstract class AbstractTransform implements Transform {

	/**
	 * This method transforms a value of the source type into a value
	 * of the target type.
	 * @param value Incoming value of source type
	 * @return Outgoing value of target type
	 * @throws TransformationException if value is an incorrect input type or
	 * the transformation fails
	 */
	public abstract Object transform(Object value) throws TransformationException;

	/**
	 * Type of the incoming value.
	 * @return Source type
	 */
	public abstract Class getSourceType();

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
	public abstract Class getTargetType();

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

	/**
	 * Flag if the transformation from source to target is
	 * a narrowing transformation that may lose information.
	 * This class returns false by default.  This method should
	 * be overridden if the transform is a narrowing transform.
	 * @return False unless overridden.
	 */
	public boolean isNarrowing() {
		return false;
	}

	/**
	 * Override Object.toString() to do getDisplayName() version.
	 * @return String representation of object
	 */
	public String toString() {
		return getDisplayName();
	}

	/**
	 * Override Object.hashCode() to build a hash based on types.
	 * @return Hash code
	 */
	public int hashCode() {
		return HashCodeUtil.hashCode( getSourceTypeName().hashCode(), getTargetTypeName().hashCode() );
	}

	/**
	 * Override Object.equals() to build an equals based on src and tgt types.
	 * @param obj Other object
	 * @return True if obj==this
	 */
	public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}

		if(! this.getClass().isInstance(obj)) {
			return false;
		}

		Transform other = (Transform) obj;
		return other.getSourceType() == this.getSourceType() &&
			   other.getTargetType() == this.getTargetType();
	}
}
