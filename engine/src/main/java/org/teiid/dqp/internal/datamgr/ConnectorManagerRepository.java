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

package org.teiid.dqp.internal.datamgr;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidException;
import org.teiid.query.QueryPlugin;
import org.teiid.translator.ExecutionFactory;


public class ConnectorManagerRepository implements Serializable{
	
	@SuppressWarnings("serial")
	public static class ConnectorManagerException extends TeiidException {
		public ConnectorManagerException(String msg) {
			super(msg);
		}
		public ConnectorManagerException(Throwable t) {
			super(t);
		}
	}
	
	public interface ExecutionFactoryProvider {
		ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException;
	}
	
	private static final long serialVersionUID = -1611063218178314458L;
	
	private Map<String, ConnectorManager> repo = new ConcurrentHashMap<String, ConnectorManager>();
	
	public void addConnectorManager(String connectorName, ConnectorManager mgr) {
		this.repo.put(connectorName, mgr);
	}
	
	public ConnectorManager getConnectorManager(String connectorName) {
		return this.repo.get(connectorName);
	}
	
	public Map<String, ConnectorManager> getConnectorManagers() {
		return repo;
	}
	
	public ConnectorManager removeConnectorManager(String connectorName) {
		return this.repo.remove(connectorName);
	}
	
	public void createConnectorManagers(VDBMetaData deployment, ExecutionFactoryProvider provider) throws ConnectorManagerException {
		for (ModelMetaData model : deployment.getModelMetaDatas().values()) {
			List<String> sourceNames = model.getSourceNames();
			if (sourceNames.size() != new HashSet<String>(sourceNames).size()) {
				throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31101, model.getName(), deployment.getName(), deployment.getVersion()));
			}
			if (sourceNames.size() > 1 && !model.isSupportsMultiSourceBindings()) {
				throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31102, model.getName(), deployment.getName(), deployment.getVersion()));
			}
			for (String source:sourceNames) {
				ConnectorManager cm = getConnectorManager(source);
				String name = model.getSourceTranslatorName(source);
				String connection = model.getSourceConnectionJndiName(source);
				if (cm != null) {
					if (!cm.getTranslatorName().equals(name)
							|| !cm.getConnectionName().equals(connection)) {
						throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31103, source, deployment.getName(), deployment.getVersion()));
					}
					continue;
				}
				cm = createConnectorManager(name, connection);
				ExecutionFactory<Object, Object> ef = provider.getExecutionFactory(name);
				cm.setExecutionFactory(ef);
				addConnectorManager(source, cm);
			}
		}
	}

	protected ConnectorManager createConnectorManager(String name,
			String connection) {
		return new ConnectorManager(name, connection);
	}
}
