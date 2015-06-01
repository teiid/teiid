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
package org.teiid.example;

import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import javax.resource.ResourceException;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jca.adapters.jdbc.local.LocalManagedConnectionFactory;
import org.jboss.jca.core.api.connectionmanager.pool.PoolConfiguration;
import org.jboss.jca.core.connectionmanager.notx.NoTxConnectionManagerImpl;
import org.jboss.jca.core.connectionmanager.pool.strategy.OnePool;

import com.arjuna.ats.arjuna.common.ObjectStoreEnvironmentBean;
import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.common.internal.util.propertyservice.BeanPopulator;

/**
 * The <code>EmbeddedHelper</code> is a Helper class for running Teiid Embedded. 
 * 
 * The 'getTransactionManager()' method return a Narayana JTA TransactionManager which used to set to EmbeddedConfiguration:
 * 
 * <pre>
 *  EmbeddedServer server = new EmbeddedServer();
 *  ...
 *  EmbeddedConfiguration config = new EmbeddedConfiguration();
 *  config.setTransactionManager(EmbeddedHelper.getTransactionManager());
 *  server.start(config);
 * </pre>
 * 
 * The 'newDataSource' method return a ironjacamar WrapperDataSource which used to set EmbeddedServer's named connection factory:
 * 
 * <pre>
 *  EmbeddedServer server = new EmbeddedServer();
 *  ...
 *  DataSource ds = EmbeddedHelper.newDataSource("${driver}", "${url}", "${user}", "${password}");
 *  server.addConnectionFactory("name", ds);
 *  ...
 * </pre>
 * 
 *
 */
public class EmbeddedHelper {
	
	public static TransactionManager getTransactionManager() throws Exception {
		
		arjPropertyManager.getCoreEnvironmentBean().setNodeIdentifier("1"); //$NON-NLS-1$
		arjPropertyManager.getCoreEnvironmentBean().setSocketProcessIdPort(0);
		arjPropertyManager.getCoreEnvironmentBean().setSocketProcessIdMaxPorts(10);
		
		arjPropertyManager.getCoordinatorEnvironmentBean().setEnableStatistics(false);
		arjPropertyManager.getCoordinatorEnvironmentBean().setDefaultTimeout(300);
		arjPropertyManager.getCoordinatorEnvironmentBean().setTransactionStatusManagerEnable(false);
		arjPropertyManager.getCoordinatorEnvironmentBean().setTxReaperTimeout(120000);
		
		String storeDir = getStoreDir();
		
		arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreDir(storeDir);
		BeanPopulator.getNamedInstance(ObjectStoreEnvironmentBean.class, "communicationStore").setObjectStoreDir(storeDir); //$NON-NLS-1$
		
		return com.arjuna.ats.jta.TransactionManager.transactionManager();
	}

	
	private static String getStoreDir() {
		String defDir = getSystemProperty("user.home") + File.separator + ".teiid/embedded/data"; //$NON-NLS-1$ //$NON-NLS-2$
		return getSystemProperty("teiid.embedded.txStoreDir", defDir); //$NON-NLS-1$
	}


	private static String getSystemProperty(final String name, final String value) {
		return AccessController.doPrivileged(new PrivilegedAction<String>(){

			@Override
			public String run() {
				return System.getProperty(name, value);
			}});
	}
	
	private static String getSystemProperty(final String name) {
		return AccessController.doPrivileged(new PrivilegedAction<String>(){

			@Override
			public String run() {
				return System.getProperty(name);
			}});
	}
	
	public static DataSource newDataSource(String driverClass, String connURL, String user, String password) throws ResourceException{

		LocalManagedConnectionFactory mcf = new LocalManagedConnectionFactory();
		
		mcf.setDriverClass(driverClass);
		mcf.setConnectionURL(connURL);
		mcf.setUserName(user);
		mcf.setPassword(password);
		
		NoTxConnectionManagerImpl cm = new NoTxConnectionManagerImpl();
		OnePool pool = new OnePool(mcf, new PoolConfiguration(), false);
		pool.setConnectionListenerFactory(cm);
		cm.setPool(pool);
		
		return (DataSource) mcf.createConnectionFactory(cm);
	}
	
	public static void enableLogger() {
        enableLogger(Level.FINEST, "org.teiid"); //$NON-NLS-1$
    }
	
	public static void enableLogger(Level level) {
        enableLogger(level, "org.teiid"); //$NON-NLS-1$
    }
    
    public static void enableLogger(Level level, String... names){
        enableLogger(new TeiidLoggerFormatter(), Level.SEVERE, level, names);
    }
    
    /**
     * This method supply function for adjusting Teiid logging while running the Embedded mode.
     * 
     * For example, with parameters names is 'org.teiid.COMMAND_LOG' and level is 'FINEST' Teiid
     * will show command logs in console.
     * 
     * @param formatter
     *          The Formatter used in ConsoleHandler 
     * @param rootLevel
     *          The default logger level is 'INFO', this parameter used to adjust default logger level
     * @param level
     *          The logger level used in Logger
     * @param names
     *          The logger's name
     */
    public static void enableLogger(Formatter formatter, Level rootLevel, Level level, String... names){
        
        Logger rootLogger = Logger.getLogger("");
        for(Handler handler : rootLogger.getHandlers()){
            handler.setFormatter(formatter);
            handler.setLevel(rootLevel);
        }
        
        for(String name : names) {
            Logger logger = Logger.getLogger(name);
            logger.setLevel(level);
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(level);
            handler.setFormatter(formatter);
            logger.addHandler(handler);
            logger.setUseParentHandlers(false);
        }
    }
    
    public static class TeiidLoggerFormatter extends Formatter {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm SSS");

        public String format(LogRecord record) {
            StringBuffer sb = new StringBuffer();
            sb.append(format.format(new Date(record.getMillis())) + " ");
            sb.append(getLevelString(record.getLevel()) + " ");
            sb.append("[" + record.getLoggerName() + "] (");
            sb.append(Thread.currentThread().getName() + ") ");
            sb.append(record.getMessage() + "\n");
            return sb.toString();
        }

        private String getLevelString(Level level) {
            String name = level.toString();
            int size = name.length();
            for(int i = size; i < 7 ; i ++){
                name += " ";
            }
            return name;
        }
    }


}
