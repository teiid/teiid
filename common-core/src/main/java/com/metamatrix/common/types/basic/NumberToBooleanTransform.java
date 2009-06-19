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

package com.metamatrix.common.types.basic;

import com.metamatrix.common.types.AbstractTransform;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;

public class NumberToBooleanTransform extends AbstractTransform {
	
	private Object trueVal;
	private Object falseVal;
	private Class<?> sourceType;
	
	public NumberToBooleanTransform(Object trueVal, Object falseVal) {
		this.trueVal = trueVal;
		this.falseVal = falseVal;
		this.sourceType = trueVal.getClass();
	}

	@Override
	public Class getSourceType() {
		return sourceType;
	}
	
	@Override
	public Class getTargetType() {
		return DataTypeManager.DefaultDataClasses.BOOLEAN;
	}
	
	@Override
	public Object transform(Object value) throws TransformationException {
		if (value == null) {
			return null;
		}
		if (value.equals(trueVal)) {
			return Boolean.TRUE;
		}
		if (value.equals(falseVal)) {
			return Boolean.FALSE;
		}
		throw new TransformationException(ErrorMessageKeys.TYPES_ERR_0013, CorePlugin.Util.getString(ErrorMessageKeys.TYPES_ERR_0013, sourceType.getSimpleName(), value));
	}

}
