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
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidException;
import org.teiid.core.util.EquivalenceUtil;
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
	
	/**
	 * Provides {@link ExecutionFactory}s to the {@link ConnectorManagerRepository}
	 */
	public interface ExecutionFactoryProvider {
		/**
		 * 
		 * @param name
		 * @return the named {@link ExecutionFactory} or throw a {@link ConnectorManagerException} if it does not exist
		 * @throws ConnectorManagerException
		 */
		ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException;
	}
	
	private static final long serialVersionUID = -1611063218178314458L;
	
	private Map<String, ConnectorManager> repo = new ConcurrentHashMap<String, ConnectorManager>();
	private boolean shared;
	private ExecutionFactoryProvider provider;
	
	public ConnectorManagerRepository() {
	}
	
	protected ConnectorManagerRepository(boolean b) {
		this.shared = b;
	}

	public boolean isShared() {
		return shared;
	}
	
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
			for (SourceMappingMetadata source : model.getSourceMappings()) {
				createConnectorManager(deployment, provider, source, false);
			}
		}
	}

	public void createConnectorManager(
			VDBMetaData deployment, ExecutionFactoryProvider provider,
			SourceMappingMetadata source, boolean replace) throws ConnectorManagerException {
		ConnectorManager cm = getConnectorManager(source.getName());
		String name = source.getTranslatorName();
		String connection = source.getConnectionJndiName();
		ExecutionFactory<Object, Object> ef = null;
		if (cm != null) {
			if (!cm.getTranslatorName().equals(name)
					|| !EquivalenceUtil.areEqual(cm.getConnectionName(), connection)) {
				if (!replace) {
					throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31103, source, deployment.getName(), deployment.getVersion()));
				}
				if (cm.getTranslatorName().equals(name)) {
					ef = cm.getExecutionFactory();
				}
			} else {
				return;
			}
		}
		if (ef == null) {
			ef = provider.getExecutionFactory(name);
			if (ef == null) {
				throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31146, deployment.getName(), deployment.getVersion(), name));
			}
		}
		cm = createConnectorManager(name, connection, ef);
		addConnectorManager(source.getName(), cm);
	}

	protected ConnectorManager createConnectorManager(String name,
			String connection, ExecutionFactory<Object, Object> ef) {
		return new ConnectorManager(name, connection, ef);
	}
	
	public void setProvider(ExecutionFactoryProvider provider) {
		this.provider = provider;
	}
	
	public ExecutionFactoryProvider getProvider() {
		return provider;
	}
}
