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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.as.controller.ModelController;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.RestWarGenerator;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.vdb.runtime.VDBKey;

public class ResteasyEnabler implements VDBLifeCycleListener, Service<Void> {
	protected final InjectedValue<ModelController> controllerValue = new InjectedValue<ModelController>();
	protected final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	final InjectedValue<VDBRepository> vdbRepoInjector = new InjectedValue<VDBRepository>();
	
	private HashMap<VDBKey, AtomicBoolean> deployed = new HashMap<VDBKey, AtomicBoolean>();
	private RestWarGenerator generator;
	private boolean started = false;
	
	public ResteasyEnabler(RestWarGenerator generator) {
		this.generator = generator;
	}
	
	@Override
	public synchronized void added(String name, CompositeVDB vdb) {
		this.deployed.put(vdb.getVDBKey(), new AtomicBoolean(false));
	}
	
	@Override
	public void beforeRemove(String name, CompositeVDB cvdb) {
		if (cvdb != null) {
			this.deployed.remove(cvdb.getVDBKey());
		}
	}	
	
	@Override
	public synchronized void finishedDeployment(String name, CompositeVDB cvdb) {
		final VDBMetaData vdb = cvdb.getVDB();
		
		if (!vdb.getStatus().equals(Status.ACTIVE) || !started) {
			return;
		}

		final String warName = buildName(name, cvdb.getVDB().getVersion());
		if (generator.hasRestMetadata(vdb) && this.deployed.get(cvdb.getVDBKey()).compareAndSet(false, true)) {
			final Runnable job = new Runnable() {
				@Override
				public void run() {
					try {
						byte[] warContents = generator.getContent(vdb);
						if (!vdb.getStatus().equals(Status.ACTIVE)) {
							return;
						}
						if (warContents != null) {
							//make it a non-persistent deployment
							getAdmin().deploy(warName, new ByteArrayInputStream(warContents), false);
						}
					} catch (IOException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e, 
								IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50109, warName)); 
					} catch (AdminException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e, 
								IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50109, warName)); 
					}
				}
			};
			getExecutor().execute(job);
		}
	}
	
	@Override
	public synchronized void removed(String name, CompositeVDB cvdb) {

	}
	
	private String buildName(String name, String version) {
		return name+"_"+version +".war"; //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Override
	public Void getValue() throws IllegalStateException, IllegalArgumentException {
		return null;
	}

	@Override
	public void start(StartContext arg0) throws StartException {
		started = true;
		this.vdbRepoInjector.getValue().addListener(this);
	}

	@Override
	public void stop(StopContext arg0) {
		started = false;
	}
	
	Admin getAdmin() {
		return AdminFactory.getInstance()
		.createAdmin(controllerValue.getValue().createClient(executorInjector.getValue()));		
	}
	
	Executor getExecutor() {
		return executorInjector.getValue();
	}
}
