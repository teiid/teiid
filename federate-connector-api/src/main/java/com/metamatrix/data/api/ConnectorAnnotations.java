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

package com.metamatrix.data.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.metamatrix.data.pool.ConnectorIdentityFactory;
import com.metamatrix.dqp.internal.datamgr.ConnectorPropertyNames;

public class ConnectorAnnotations {
	
	/**
	 * The Pooling Annotation can be used to enable/suppress automatic pooling.
	 * 
	 * This is especially useful in situations where legacy ConnectionPool properties 
	 * are still in use and thus {@link ConnectorPropertyNames#CONNECTION_POOL_ENABLED}
	 * is set to true or when a connector cannot possibly provide a proper implementation
	 * of a {@link ConnectorIdentityFactory}.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.TYPE})
	public @interface ConnectionPooling {
		boolean enabled() default true;
	}

}
