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
package com.metamatrix.common.util;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.metamatrix.core.MetaMatrixCoreException;

public class JMXUtil {
	public enum MBeanType {SERVICE, SERVER, UTIL, ADMINAPI};
	
	String processName;
	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
	
	public JMXUtil(String processName) {
		this.processName = processName;
	}
	
	public ObjectName buildName(MBeanType type, String name) throws FailedToRegisterException {
		try {
			return new ObjectName("Teiid["+processName+"]:type="+type+",name="+name); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$		
		} catch (MalformedObjectNameException e) {
			throw new FailedToRegisterException(e);
		}
	}

	public void register(MBeanType type, String name, Object anObj) throws FailedToRegisterException {
		try {
			mbs.registerMBean(anObj, buildName(type, name));
		} catch (InstanceAlreadyExistsException e) {
			throw new FailedToRegisterException(e);
		} catch (MBeanRegistrationException e) {
			throw new FailedToRegisterException(e);
		} catch (NotCompliantMBeanException e) {
			throw new FailedToRegisterException(e);
		}
	}
	
	public void unregister(MBeanType type, String name) throws FailedToRegisterException {
 		try {
			mbs.unregisterMBean(buildName(type, name));
		} catch (InstanceNotFoundException e) {
			throw new FailedToRegisterException(e);
		} catch (MBeanRegistrationException e) {
			throw new FailedToRegisterException(e);
		}
	}

	public MBeanServer getMBeanServer() {
		return mbs;
	}
	
	public static class FailedToRegisterException extends MetaMatrixCoreException{
		public FailedToRegisterException(Throwable e) {
			super(e);
		}
	}
	
}
