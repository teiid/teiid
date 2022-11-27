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

package org.teiid.jdbc.jboss;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.jdbc.LocalProfile;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;

public class ModuleLocalProfile implements LocalProfile {

    static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$

    @Override
    public ConnectionImpl connect(String url, Properties info)
        throws TeiidSQLException {
        try {
            ServerConnection sc = createServerConnection(info);
            return new ConnectionImpl(sc, info, url);
        } catch (TeiidRuntimeException e) {
            throw TeiidSQLException.create(e);
        } catch (TeiidException e) {
            throw TeiidSQLException.create(e);
        } catch (LinkageError e) {
            throw TeiidSQLException.create(e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20039));
        }
    }

    @Override
    public ServerConnection createServerConnection(Properties info)
            throws TeiidException {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            ModuleLoader callerModuleLoader = Module.getCallerModuleLoader();
            if (callerModuleLoader == null) {
                logger.fine(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20033));
                return (ServerConnection)ReflectionHelper.create("org.teiid.transport.LocalServerConnection", Arrays.asList(info, PropertiesUtils.getBooleanProperty(info, LocalProfile.USE_CALLING_THREAD, true)), Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
            }
            final Module module = callerModuleLoader.loadModule("org.jboss.teiid"); //$NON-NLS-1$
            Thread.currentThread().setContextClassLoader(module.getClassLoader());
            return (ServerConnection)ReflectionHelper.create("org.teiid.transport.LocalServerConnection", Arrays.asList(info, PropertiesUtils.getBooleanProperty(info, LocalProfile.USE_CALLING_THREAD, true)), Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
        } catch (ModuleLoadException e) {
             throw new ConnectionException(JDBCPlugin.Event.TEIID20008, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20008));
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
    }

}
