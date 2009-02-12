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

package com.metamatrix.metadata.runtime.impl;

import com.metamatrix.modeler.core.metadata.runtime.PropertyValueRecord;

/**
 * PropertyValueRecordImpl
 */
public class PropertyValueRecordImpl implements PropertyValueRecord {

	private String property;
	private String value;
    
	// ==================================================================================
	//                        C O N S T R U C T O R S
	// ==================================================================================
    
	public PropertyValueRecordImpl(final String property, final String value) {
		this.property     = property;
		this.value = value;
	}

	//==================================================================================
	//                     I N T E R F A C E   M E T H O D S
	//==================================================================================

	/* (non-Javadoc)
	 * @see com.metamatrix.modeler.core.metadata.runtime.PropertyValueRecord#getProperty()
	 */
	public String getProperty() {
		return property;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.modeler.core.metadata.runtime.PropertyValueRecord#getValue()
	 */
	public String getValue() {
		return value;
	}

}
