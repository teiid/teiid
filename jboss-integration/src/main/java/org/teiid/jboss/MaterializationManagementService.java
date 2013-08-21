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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.Executor;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.MaterializationManager;

class MaterializationManagementService implements Service<MaterializationManager> {
	
	private Timer timer;
	private MaterializationManager manager;
	protected final InjectedValue<DQPCore> dqpInjector = new InjectedValue<DQPCore>();
	protected final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	protected final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private JBossLifeCycleListener shutdownListener;
	
	public MaterializationManagementService(JBossLifeCycleListener shutdownListener) {
		this.shutdownListener = shutdownListener;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		timer = new Timer("Teiid Timer", true); //$NON-NLS-1$
		manager = new MaterializationManager(shutdownListener) {
			
			@Override
			public Timer getTimer() {
				return timer;
			}
			
			@Override
			public Executor getExecutor() {
				return executorInjector.getValue();
			}
			
			@Override
			public List<Map<String, String>> executeQuery(VDBMetaData vdb, String cmd) throws SQLException {
				try {
					ModelNode results = new ModelNode();
					LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50100, "executing="+cmd)); //$NON-NLS-1$
					TeiidOperationHandler.executeQuery(vdb, dqpInjector.getValue(), cmd, -1, results, false);
					ArrayList<Map<String, String>> rows = new ArrayList<Map<String,String>>();
					int i = 0;
					while(true) {
						if (results.get(i).isDefined()) {
							HashMap<String, String> row = new HashMap();
							List<ModelNode> cols = results.get(i).asList();
							for (ModelNode col:cols) {
			        			if (!col.getType().equals(ModelType.PROPERTY)) {
			        				continue;
			        			}
			    				org.jboss.dmr.Property p = col.asProperty();
			    				row.put(p.getName(), p.getValue().asString());
							}
							rows.add(row);
							i++;
						}
						else {
							break;
						}
					}
					LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50100, results));
					return rows;
				} catch(Throwable t) {
					throw new TeiidSQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50100, t));
				}
			}
		};
		
		vdbRepositoryInjector.getValue().addListener(manager);
	}

	@Override
	public void stop(StopContext context) {
		timer.cancel();
		vdbRepositoryInjector.getValue().removeListener(manager);
	}

	@Override
	public MaterializationManager getValue() throws IllegalStateException, IllegalArgumentException {
		return this.manager;
	}	
}
