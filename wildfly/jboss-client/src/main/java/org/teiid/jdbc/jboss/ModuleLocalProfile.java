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
