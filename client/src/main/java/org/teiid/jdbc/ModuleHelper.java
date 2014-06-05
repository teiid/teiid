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

package org.teiid.jdbc;

import java.util.Arrays;
import java.util.Properties;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.teiid.core.TeiidException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;

/**
 * This class isolates the dependency on JBoss modules
 */
class ModuleHelper {
	
	static ServerConnection createFromModule(Properties info)
			throws ConnectionException, TeiidException {
		ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
        	ModuleLoader callerModuleLoader = Module.getCallerModuleLoader();
        	if (callerModuleLoader == null) {
        		throw new ConnectionException(JDBCPlugin.Event.TEIID20033, null, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20033));
        	}
			final Module module = callerModuleLoader.loadModule(ModuleIdentifier.create("org.jboss.teiid")); //$NON-NLS-1$
        	Thread.currentThread().setContextClassLoader(module.getClassLoader());
        	return (ServerConnection)ReflectionHelper.create("org.teiid.transport.LocalServerConnection", Arrays.asList(info, PropertiesUtils.getBooleanProperty(info, EmbeddedProfile.USE_CALLING_THREAD, true)), Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
        } catch (ModuleLoadException e) {
        	 throw new ConnectionException(JDBCPlugin.Event.TEIID20008, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20008));
		} finally {
        	Thread.currentThread().setContextClassLoader(tccl);
        }
	}

}
