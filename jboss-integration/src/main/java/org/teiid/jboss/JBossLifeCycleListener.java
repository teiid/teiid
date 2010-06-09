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
package org.teiid.jboss;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.jboss.system.ServiceMBeanSupport;
import org.teiid.deployers.ContainerLifeCycleListener;

public class JBossLifeCycleListener extends ServiceMBeanSupport implements NotificationListener, ContainerLifeCycleListener{

	public final String START_NOTIFICATION_TYPE = "org.jboss.system.server.started"; //$NON-NLS-1$
	/** The JMX notification event type sent on begin of the server shutdown */
	public final String STOP_NOTIFICATION_TYPE = "org.jboss.system.server.stopped"; //$NON-NLS-1$
	
	private boolean shutdownInProgress = false;
	
	public JBossLifeCycleListener() {
		try {
			MBeanServer server = MBeanServerFactory.findMBeanServer(null).get(0);
			ObjectName on = new ObjectName("jboss.system:type=Server"); //$NON-NLS-1$
			server.addNotificationListener(on, this, null, null);
		} catch (MalformedObjectNameException e) {
			//ignore
		} catch (InstanceNotFoundException e) {
			//ignore
		} 
	}
	
	@Override
	public void handleNotification(Notification msg, Object handback) {
		String type = msg.getType();
		if (type.equals(START_NOTIFICATION_TYPE)) {
		}
		
		if (type.equals(STOP_NOTIFICATION_TYPE)) {
			shutdownInProgress = true;
		}	
	}

	@Override
	public boolean isShutdownInProgress() {
		return shutdownInProgress;
	}
}
