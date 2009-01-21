/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.config;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.jdbc.SimplePooledConnectionSource;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * Created on May 14, 2002
 *
 * The JDBCResourcePool is used to obtain a JDBC Connection from the
 * resource pool.
 */


public final class JDBCConnectionPoolHelper {
	
	private static SimplePooledConnectionSource INSTANCE;
			
	public static synchronized SimplePooledConnectionSource getInstance() {
		if (INSTANCE == null) {
			try {
				INSTANCE = new SimplePooledConnectionSource(CurrentConfiguration.getBootStrapProperties());
			} catch (ConfigurationException e) {
				throw new MetaMatrixRuntimeException(e);
			}
		}
		return INSTANCE;
	}
	
}
