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

package com.metamatrix.common.application;

import java.util.Map;

import com.google.inject.AbstractModule;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.service.DQPServiceNames;

public class DQPGuiceModule extends AbstractModule {

	private DQPConfigSource configSource;
	
	public DQPGuiceModule(DQPConfigSource configSource) {
		this.configSource = configSource;
	}
	
	@Override
	protected void configure() {
		Map<String, Class<? extends ApplicationService>> defaults = configSource.getDefaultServiceClasses();
		for(int i=0; i<DQPServiceNames.ALL_SERVICES.length; i++) {
			final String serviceName = DQPServiceNames.ALL_SERVICES[i];
			String className = configSource.getProperties().getProperty("service."+serviceName+".classname"); //$NON-NLS-1$ //$NON-NLS-2$
			Class clazz = defaults.get(serviceName);
			if (clazz != null && className != null) {
				try {
					clazz = Class.forName(className);
				} catch (ClassNotFoundException e) {
					throw new MetaMatrixRuntimeException(e);
				}
			}
			if (clazz != null) {
				bind(DQPServiceNames.ALL_SERVICE_CLASSES[i]).to(clazz);
			}
		}
	}

}
