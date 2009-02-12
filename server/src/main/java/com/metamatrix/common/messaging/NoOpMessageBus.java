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

import com.metamatrix.core.event.EventObjectListener;

/**
 * A MessageBus implementation - all methods do nothing.  For testing,
 * or for running an object standalone, only.
 */
public class NoOpMessageBus implements MessageBus {


    public NoOpMessageBus() {
    }

    public void addListener(Class eventClass, EventObjectListener listener)
        throws MessagingException  {

    }

    public void processEvent(EventObject obj) throws MessagingException {

    }

    public void shutdown() throws MessagingException {

    }

    public synchronized void removeListener(Class eventClass, EventObjectListener listener)
        throws MessagingException  {

    }

    public synchronized void removeListener(EventObjectListener listener)
        throws MessagingException  {

    }

	public Serializable export(Object object, Class[] targetClasses) {
		return new EventObject(object);
	}

	public Object getRPCProxy(Object object) {
		return ((EventObject)object).getSource();
	}

	public void unExport(Object object) {
		
	}
}

