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

package com.metamatrix.common.messaging;

import java.io.Serializable;
import java.util.EventObject;
import java.util.Properties;

import org.jgroups.ChannelException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.messaging.jgroups.JGroupsMessageBus;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.event.AsynchEventBroker;
import com.metamatrix.core.event.EventBroker;
import com.metamatrix.core.event.EventBrokerException;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.event.EventSourceException;
import com.metamatrix.server.ChannelProvider;
import com.metamatrix.server.Configuration;

@Singleton
public class VMMessageBus implements MessageBus {

    private Object messageBus;  
    private Object lock = new Object();
    private boolean closed = true;

    private EventBroker eventBroker = new AsynchEventBroker("VMMessageBus"); //$NON-NLS-1$
    
    @Inject
    public VMMessageBus(ChannelProvider channelProvider, @Named(Configuration.CLUSTERNAME) String clusterName) throws MetaMatrixComponentException {
        Properties env = null;
        // when the old messagebus Resource was replaced with the JGroups resource,
        // the MESSAGE_BUS_TYPE property was moved to the global properties section
        // however, (HERES THE HACK), CurrentConfiguration.getInstance().getProperties() does not
        // allow the system properties to override configuration settings, therefore,
        // the MESSAGE_BUS_TYPE property could not be overridden with TYPE_NOOP
        // so were looking at System.getProperty() to force the override
        String mbType = System.getProperty(MessageBusConstants.MESSAGE_BUS_TYPE);
        
        if (mbType == null || mbType.trim().length() == 0) {
            env = CurrentConfiguration.getInstance().getProperties();
            mbType = env.getProperty(MessageBusConstants.MESSAGE_BUS_TYPE);
        }

        if (mbType != null && mbType.equals(MessageBusConstants.TYPE_NOOP)) {
            messageBus = new NoOpMessageBus();
        } else {
            try {
				messageBus = new JGroupsMessageBus(channelProvider, eventBroker, clusterName);
			} catch (ChannelException e) {
				throw new MetaMatrixComponentException(e);
			}
        }
        closed = false;
    }
    
    public void addListener(Class eventClass, EventObjectListener listener) throws MessagingException {
        synchronized (lock) {
	    	if (closed) {
	    		return;
	    	}
	        try {
	            eventBroker.addListener(eventClass, listener);
	        } catch (EventSourceException e) {
	            throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0013, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0013));
	        }
        }
    }

    public void shutdown() throws MessagingException {
    	synchronized (lock) {
    		if (closed) {
    			return;
    		}
    		closed = true;
    		((MessageBus)messageBus).shutdown();
    		try {
    			eventBroker.shutdown();
    		} catch (EventBrokerException e) {
    			throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0014, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0014));
    		}
    		messageBus = null;
    		eventBroker = null;
    	}
    }

    public void removeListener(Class eventClass, EventObjectListener listener)
        throws MessagingException {

    	synchronized (lock) {
    		if (closed) {
    			return;
    		}
    		try {
    			eventBroker.removeListener(eventClass, listener);
    		} catch (EventSourceException e) {
    			throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0015, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0015));
    		}
    	}
    }

    public void removeListener(EventObjectListener listener)
        throws MessagingException {

        synchronized (lock) {
        	if (closed) {
        		return;
        	}
        	try {
        		eventBroker.removeListener(listener);
        	} catch (EventSourceException e) {
        		throw new MessagingException(e, ErrorMessageKeys.MESSAGING_ERR_0015, CommonPlugin.Util.getString(ErrorMessageKeys.MESSAGING_ERR_0015));
        	}
        }
    }

    public void processEvent(EventObject obj) throws MessagingException {
        synchronized (lock) {
        	if (closed) {
        		return;
        	}
        	((MessageBus)messageBus).processEvent(obj);
        	eventBroker.processEvent(obj);
        }
    }

	public Serializable export(Object object, Class[] targetClasses) {
		synchronized (lock) {
        	if (closed) {
        		return null;
        	}
    		return ((MessageBus)messageBus).export(object, targetClasses);
        }
	}

	public Object getRPCProxy(Object object) {
		synchronized (lock) {
        	if (closed) {
        		return null;
        	}
    		return ((MessageBus)messageBus).getRPCProxy(object);
        }
	}

	public void unExport(Object object) {
		synchronized (lock) {
        	if (closed) {
        		return;
        	}
        	((MessageBus)messageBus).unExport(object);
        }
	}
}

