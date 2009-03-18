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

package org.teiid.dqp.internal.datamgr.impl;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.teiid.connector.api.TypeFacility;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.util.TimestampWithTimezone;

/**
 */
public class TypeFacilityImpl extends TypeFacility {

    public TypeFacilityImpl() {
    }

	@Override
	public Object convertToRuntimeType(Object value) {
		return DataTypeManager.convertToRuntimeType(value);
	}
	
	@Override
	public Object convertDate(Date date, TimeZone initial, Calendar target,
			Class targetType) {
		return TimestampWithTimezone.create(date, initial, target, targetType);
	}

}
