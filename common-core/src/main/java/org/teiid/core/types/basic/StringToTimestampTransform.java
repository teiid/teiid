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

package org.teiid.core.types.basic;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;


public class StringToTimestampTransform extends Transform {
	
	private static boolean validate = true;
	private static Pattern pattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?"); //$NON-NLS-1$
	
	static {
		try {
			Timestamp.valueOf("2000-14-01 00:00:00"); //$NON-NLS-1$
		} catch (Exception e) {
			validate = false;
		}
	}

	/**
	 * This method transforms a value of the source type into a value
	 * of the target type.
	 * @param value Incoming value of source type
	 * @return Outgoing value of target type
	 * @throws TransformationException if value is an incorrect input type or
	 * the transformation fails
	 */
	public Object transformDirect(Object value) throws TransformationException {
		String val = ((String) value).trim();
		Timestamp result = null;
		try {
			result = Timestamp.valueOf( val );
		} catch(Exception e) {
		    if (!validate && pattern.matcher(val).matches()) {
				throw new TransformationException(CorePlugin.Event.TEIID10060, CorePlugin.Util.gs(CorePlugin.Event.TEIID10060, value, getTargetType().getSimpleName()));
			}
			  throw new TransformationException(CorePlugin.Event.TEIID10059, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10059, value));
		}
		//validate everything except for fractional seconds
		String substring = result.toString().substring(0, 19);
		if (!val.startsWith(substring)) {
			TimeZone tz = TimeZone.getDefault();
			if (tz.useDaylightTime()) {
				//check for a transition with a more costly SimpleDateFormat using a non-DST timezone
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //$NON-NLS-1$
				sdf.setLenient(false);
				sdf.setTimeZone(TimeZone.getTimeZone("GMT")); //$NON-NLS-1$
				try {
					sdf.parse(val.substring(0, 19));
					return result;
				} catch (ParseException e) {
					//let the exception happen
				}
			}
			throw new TransformationException(CorePlugin.Event.TEIID10060, CorePlugin.Util.gs(CorePlugin.Event.TEIID10060, value, getTargetType().getSimpleName()));
		}
		return result;
	}

	/**
	 * Type of the incoming value.
	 * @return Source type
	 */
	public Class<?> getSourceType() {
		return String.class;
	}

	/**
	 * Type of the outgoing value.
	 * @return Target type
	 */
	public Class<?> getTargetType() {
		return Timestamp.class;
	}
	
	@Override
	public boolean isExplicit() {
		return true;
	}

}
