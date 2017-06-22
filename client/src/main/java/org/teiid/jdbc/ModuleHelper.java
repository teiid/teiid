/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

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
    
    static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
	
	static ServerConnection createFromModule(Properties info)
			throws ConnectionException, TeiidException {
		ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
        	ModuleLoader callerModuleLoader = Module.getCallerModuleLoader();
        	if (callerModuleLoader == null) {
        	    logger.fine(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20033));
        	    return (ServerConnection)ReflectionHelper.create("org.teiid.transport.LocalServerConnection", Arrays.asList(info, PropertiesUtils.getBooleanProperty(info, LocalProfile.USE_CALLING_THREAD, true)), Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
        	} 
			final Module module = callerModuleLoader.loadModule(ModuleIdentifier.create("org.jboss.teiid")); //$NON-NLS-1$
        	Thread.currentThread().setContextClassLoader(module.getClassLoader());
        	return (ServerConnection)ReflectionHelper.create("org.teiid.transport.LocalServerConnection", Arrays.asList(info, PropertiesUtils.getBooleanProperty(info, LocalProfile.USE_CALLING_THREAD, true)), Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
        } catch (ModuleLoadException e) {
        	 throw new ConnectionException(JDBCPlugin.Event.TEIID20008, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20008));
		} finally {
        	Thread.currentThread().setContextClassLoader(tccl);
        }
	}

}
