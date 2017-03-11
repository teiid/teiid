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

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.runtime.MaterializationManager;
import org.teiid.runtime.NodeTracker;

class MaterializationManagementService implements Service<MaterializationManager> {
	private ScheduledExecutorService scheduler;
	private MaterializationManager manager;
	protected final InjectedValue<DQPCore> dqpInjector = new InjectedValue<DQPCore>();
	protected final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	protected final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	protected final InjectedValue<NodeTracker> nodeTrackerInjector = new InjectedValue<NodeTracker>();
	private JBossLifeCycleListener shutdownListener;
	
	public MaterializationManagementService(JBossLifeCycleListener shutdownListener, ScheduledExecutorService scheduler) {
		this.shutdownListener = shutdownListener;
		this.scheduler = scheduler;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		manager = new MaterializationManager(shutdownListener) {
			@Override
			public ScheduledExecutorService getScheduledExecutorService() {
				return scheduler;
			}
			
			@Override
			public Executor getExecutor() {
				return executorInjector.getValue();
			}
			
			@Override
			public DQPCore getDQP() {
				return dqpInjector.getValue();
			}

            @Override
            public VDBRepository getVDBRepository() {
                return vdbRepositoryInjector.getValue();
            }
		};
		
		vdbRepositoryInjector.getValue().addListener(manager);
		
		if (nodeTrackerInjector.getValue() != null) {
		    nodeTrackerInjector.getValue().addNodeListener(manager);
		}
	}

	@Override
	public void stop(StopContext context) {
		scheduler.shutdownNow();
		vdbRepositoryInjector.getValue().removeListener(manager);
		NodeTracker value = nodeTrackerInjector.getValue();
		if (value != null) {
		    value.removeNodeListener(manager);
		}
	}

	@Override
	public MaterializationManager getValue() throws IllegalStateException, IllegalArgumentException {
		return this.manager;
	}	
}
