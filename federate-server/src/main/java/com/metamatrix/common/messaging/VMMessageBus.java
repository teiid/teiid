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

package com.metamatrix.common.messaging;

import java.io.Serializable;
import java.util.EventObject;
import java.util.Properties;

import org.jgroups.Channel;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.messaging.jgroups.JGroupsMessageBus;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.event.AsynchEventBroker;
import com.metamatrix.core.event.EventBroker;
import com.metamatrix.core.event.EventBrokerException;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.event.EventSourceException;

@Singleton
public class VMMessageBus implements MessageBus {

    private MessageBus messageBus;
    private Object lock = new Object();
    private boolean closed = true;

    private EventBroker eventBroker = new AsynchEventBroker("VMMessageBus"); //$NON-NLS-1$
    
    // this value gets injected using the setter method. Since we can create a NoOP bus, 
    // we are not passing in constructor 
    private Channel channel;
    
    public VMMessageBus() {
        
        synchronized (lock) {
            Properties env = null;
            try {
                // when the old messagebus Resource was replaced with the JGroups resource,
                // the MESSAGE_BUS_TYPE property was moved to the global properties section
                // however, (HERES THE HACK), CurrentConfiguration.getProperties() does not
                // allow the system properties to override configuration settings, therefore,
                // the MESSAGE_BUS_TYPE property could not be overridden with TYPE_NOOP
                // so were looking at System.getProperty() to force the override
                String mbType = System.getProperty(MessageBusConstants.MESSAGE_BUS_TYPE);
                
                if (mbType == null || mbType.trim().length() == 0) {
                    env = CurrentConfiguration.getProperties();
                    mbType = env.getProperty(MessageBusConstants.MESSAGE_BUS_TYPE);
                }

                if (mbType != null && mbType.equals(MessageBusConstants.TYPE_NOOP)) {
                    messageBus = new NoOpMessageBus();
                } else {
                    messageBus = new JGroupsMessageBus(channel, eventBroker);
                }
                closed = false;
                    
            } catch (Exception e) {
                env = new Properties();
            }
        }
    }
    
    @Inject
    void setChannel(Channel channel) {
    	this.channel = channel;
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
    		messageBus.shutdown();
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
        	messageBus.processEvent(obj);
        	eventBroker.processEvent(obj);
        }
    }

	public Serializable export(Object object, Class[] targetClasses) {
		synchronized (lock) {
        	if (closed) {
        		return null;
        	}
    		return messageBus.export(object, targetClasses);
        }
	}

	public Object getRPCProxy(Object object) {
		synchronized (lock) {
        	if (closed) {
        		return null;
        	}
    		return messageBus.getRPCProxy(object);
        }
	}

	public void unExport(Object object) {
		synchronized (lock) {
        	if (closed) {
        		return;
        	}
        	messageBus.unExport(object);
        }
	}
}

