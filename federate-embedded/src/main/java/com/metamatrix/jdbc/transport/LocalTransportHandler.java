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

package com.metamatrix.jdbc.transport;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.application.ClientConnectionListener;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.embedded.DQPEmbeddedManager;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.DQPListener;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.jdbc.EmbeddedDataSource;
import com.metamatrix.jdbc.JDBCPlugin;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

public class LocalTransportHandler implements ServerConnectionFactory {

    private final class LocalServerConnection implements ServerConnection {
		private final LogonResult result;
		private boolean shutdown;
		private DQPWorkContext workContext;

		private LocalServerConnection(Properties connectionProperties) {
			result = new LogonResult(new MetaMatrixSessionID(SESSION_ID
					.getAndIncrement()), connectionProperties
					.getProperty(MMURL.CONNECTION.USER_NAME), connectionProperties, -1, "local"); //$NON-NLS-1$
			
			//Initialize the workContext
			workContext = new DQPWorkContext();
			workContext.setSessionId(result.getSessionID());
			workContext.setVdbName(connectionProperties.getProperty(MMURL.JDBC.VDB_NAME));
			workContext.setVdbVersion(connectionProperties.getProperty(MMURL.JDBC.VDB_VERSION));
			DQPWorkContext.setWorkContext(workContext);
			if (configurationConnectionListener != null) {
				configurationConnectionListener.connectionAdded(this);
			}
		}

		public <T> T getService(Class<T> iface) {
			if (iface != ClientSideDQP.class) {
				throw new IllegalArgumentException("unknown service"); //$NON-NLS-1$
			}
			final ClientSideDQP dqp = getManager().getDQP();
			return (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {ClientSideDQP.class}, new InvocationHandler() {

				public Object invoke(Object arg0, Method arg1, Object[] arg2)
						throws Throwable {
					
					if (!isOpen()) {
						throw ExceptionUtil.convertException(arg1, new MetaMatrixComponentException(JDBCPlugin.Util.getString("LocalTransportHandler.session_inactive"))); //$NON-NLS-1$
					}
					
					DQPWorkContext.setWorkContext(workContext);
					try {
						return arg1.invoke(dqp, arg2);
					} catch (InvocationTargetException e) {
						throw e.getTargetException();
					}
				}
			});
		}

		public boolean isOpen() {
			return !shutdown && isAlive();
		}

		public void shutdown() {
			if (shutdown) {
				return;
			}
			if (configurationConnectionListener != null) {
				configurationConnectionListener.connectionRemoved(this);
			}
			if (connectionListener != null) {
				connectionListener.connectionRemoved(this);
			}
			this.shutdown = true;
		}

		public LogonResult getLogonResult() {
			return result;
		}
	}

	private static final String SERVER_CONFIG_FILE_EXTENSION = ".properties"; //$NON-NLS-1$
    
    private AtomicLong SESSION_ID = new AtomicLong(1);
    private ClientConnectionListener configurationConnectionListener;
    private DQPEmbeddedManager dqpManager;
    private DQPListener dqpListener = null;
    private ClientConnectionListener connectionListener = null;
    
    // Keeps track of whether this transport has been shutdown.  
    // After it is shutdown connections may still have a reference to it.
    private boolean alive = true;
    
    public LocalTransportHandler() {
        this(null,null);
    }    
    
    /**
     * Default constructor - used by reflection to create a new instance. 
     */
    public LocalTransportHandler(DQPListener dqpListener, ClientConnectionListener connListener) {
        this.dqpListener = dqpListener;
        this.connectionListener = connListener;
    }
    
    public synchronized boolean isAlive() {
        return alive && (dqpManager != null && dqpManager.isDQPAlive());
    }
    
    private void checkAlive() {
        if (!isAlive()) {
            throw new MetaMatrixRuntimeException(JDBCPlugin.Util.getString("LocalTransportHandler.Transport_shutdown")); //$NON-NLS-1$
        }
    }
    
    private synchronized void initManager(URL dqpURL, Properties connProperties) throws ApplicationInitializationException {
        if(dqpManager != null) {
        	return;
        }
            
        if(dqpURL == null) {
            throw new ApplicationInitializationException(JDBCPlugin.Util.getString("LocalTransportHandler.No_configuration_file_set_in_property", DQPEmbeddedProperties.DQP_BOOTSTRAP_PROPERTIES_FILE)); //$NON-NLS-1$
        }

        String dqpFileName = dqpURL.toString().toLowerCase(); 
        if (!dqpFileName.endsWith(SERVER_CONFIG_FILE_EXTENSION)) {
            throw new ApplicationInitializationException(JDBCPlugin.Util.getString("LocalTransportHandler.Invalid_config_file_extension", dqpFileName) ); //$NON-NLS-1$                    
        }
                                           
        dqpManager = new DQPEmbeddedManager(dqpURL, connProperties, this.dqpListener);
        dqpManager.createDQP();
        ConfigurationService configService = (ConfigurationService)dqpManager.getDQP().getEnvironment().findService(DQPServiceNames.CONFIGURATION_SERVICE);
        if (configService != null) {
            try {
            	configurationConnectionListener = configService.getConnectionListener();
            }catch(MetaMatrixComponentException e) {
            	configurationConnectionListener = null;
            }
        }
    }

    /**
     * @see com.metamatrix.jdbc.transport.TransportHandler#shutdown()
     */
    public synchronized void shutdown() {
        alive = false;
        if (dqpManager != null) {
            try {
                dqpManager.shutdown();
            } catch (ApplicationLifecycleException e) {
                // TODO ignore?
            }
            dqpManager = null;
        }
    }
    
    public synchronized DQPEmbeddedManager getManager() {
    	checkAlive();
        return dqpManager;
    }
    
    synchronized void setManager(DQPEmbeddedManager manager) {
        dqpManager = manager;
    }

	public ServerConnection createConnection(final Properties connectionProperties) throws CommunicationException,
			ConnectionException {
		initManager(connectionProperties);
        
        return new LocalServerConnection(connectionProperties);
	}

	public void initManager(final Properties connectionProperties)
			throws ConnectionException {
		try {
            URL dqpURL = (URL)connectionProperties.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE);
            initManager(dqpURL, connectionProperties);
        } catch(ApplicationInitializationException e) {
            throw new ConnectionException(e);
        }
	}
}
