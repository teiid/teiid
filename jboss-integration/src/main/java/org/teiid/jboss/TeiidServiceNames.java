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

import org.jboss.msc.service.ServiceName;
import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidException;
import org.teiid.deployers.VDBStatusChecker;

public class TeiidServiceNames {
	public static ServiceName ENGINE = ServiceName.JBOSS.append("teiid", "query-engine"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName TRANSLATOR_REPO = ServiceName.JBOSS.append("teiid", "translator-repository");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName VDB_REPO = ServiceName.JBOSS.append("teiid", "vdb-repository");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName TRANSLATOR_BASE = ServiceName.JBOSS.append("teiid", "translator");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName TRANSPORT_BASE = ServiceName.JBOSS.append("teiid", "transport");//$NON-NLS-1$ //$NON-NLS-2$
	private static ServiceName EMBEDDED_TRANSPORT_BASE = ServiceName.JBOSS.append("teiid", "embedded", "transport");//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	public static ServiceName BUFFER_DIR = ServiceName.JBOSS.append("teiid", "buffer.dir");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName DATA_DIR = ServiceName.JBOSS.append("teiid", "data.dir");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName BUFFER_MGR = ServiceName.JBOSS.append("teiid", "buffer-mgr");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName TUPLE_BUFFER = ServiceName.JBOSS.append("teiid", "tuple_buffer");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName AUTHORIZATION_VALIDATOR = ServiceName.JBOSS.append("teiid", "authorization-validator");//$NON-NLS-1$ //$NON-NLS-2$
	private static ServiceName VDB_SVC_BASE = ServiceName.JBOSS.append("teiid", "vdb"); //$NON-NLS-1$ //$NON-NLS-2$
	private static ServiceName VDB_FINISHED_SVC_BASE = ServiceName.JBOSS.append("teiid", "vdb-finished"); //$NON-NLS-1$ //$NON-NLS-2$
	private static ServiceName VDB_SWITCH_SVC_BASE = ServiceName.JBOSS.append("teiid", "switch"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName OBJECT_SERIALIZER = ServiceName.JBOSS.append("teiid", "object-serializer"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName CACHE_RESULTSET = ServiceName.JBOSS.append("teiid", "cache", "resultset"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	public static ServiceName CACHE_PREPAREDPLAN = ServiceName.JBOSS.append("teiid", "cache", "prepared-plan"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	public static ServiceName OBJECT_REPLICATOR = ServiceName.JBOSS.append("teiid", "object-replicator"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName VDB_STATUS_CHECKER = ServiceName.JBOSS.append("teiid", "vdb-status-checker"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName DS_LISTENER_BASE = ServiceName.JBOSS.append("teiid", "ds-listener"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName EVENT_DISTRIBUTOR_FACTORY = ServiceName.JBOSS.append("teiid", "event-distributor-factory");//$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName RESULTSET_CACHE_FACTORY = ServiceName.JBOSS.append("teiid", "infinispan-rs-cache-factory"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName PREPAREDPLAN_CACHE_FACTORY = ServiceName.JBOSS.append("teiid", "infinispan-pp-cache-factory"); //$NON-NLS-1$ //$NON-NLS-2$
	public static ServiceName MATVIEW_SERVICE = ServiceName.JBOSS.append("teiid", "matview-service"); //$NON-NLS-1$ //$NON-NLS-2$
	
	public static class InvalidServiceNameException extends TeiidException {

		private static final long serialVersionUID = 7555741825606486101L;
		
		public InvalidServiceNameException(BundleUtil.Event code, Throwable t, final String message) {
			super(code, t, message);
		}
		
	}
	
	public static ServiceName translatorServiceName(String name) {
		return ServiceName.of(TRANSLATOR_BASE, name);
	}
	
	public static ServiceName vdbServiceName(String vdbName, int version) {
		return VDB_SVC_BASE.append(vdbName, String.valueOf(version)); 
	}
	
	public static ServiceName vdbFinishedServiceName(String vdbName, int version) {
		return VDB_FINISHED_SVC_BASE.append(vdbName, String.valueOf(version)); 
	}
	
	public static ServiceName vdbSwitchServiceName(String vdbName, int version) {
		return VDB_SWITCH_SVC_BASE.append(vdbName, String.valueOf(version)); 
	}	
	
	public static ServiceName executorServiceName(String poolName) {
		return ServiceName.JBOSS.append("thread", "executor", poolName); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public static ServiceName transportServiceName(String name) {
		return ServiceName.of(TRANSPORT_BASE, name);
	}
	
	public static ServiceName embeddedTransportServiceName(String name) {
		return EMBEDDED_TRANSPORT_BASE.append(name);
	}	
	
	public static ServiceName dsListenerServiceName(String vdbName, int version, String name) throws InvalidServiceNameException {
		try {
			return ServiceName.of(DS_LISTENER_BASE, vdbName, String.valueOf(version), VDBStatusChecker.stripContext(name));
		} catch (RuntimeException e) {
			throw new InvalidServiceNameException(IntegrationPlugin.Event.TEIID50099, e, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50099, name, vdbName, version));
		}
	}
}
