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

package org.teiid.metadata;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.BundleUtil.Event;

public class MetadataException extends TeiidRuntimeException {
	private static final long serialVersionUID = -7889770730039591817L;

	public MetadataException(Event event, Throwable cause) {
		super(event, cause);
	}
	
	public MetadataException(Throwable cause) {
		super(cause);
	}
	
	public MetadataException(Event event, String message) {
		super(event, message);
	}
	
	public MetadataException(String message) {
		super(message);
	}

	public MetadataException(Event event,
			Throwable e, String message) {
		super(event, e, message);
	}
}