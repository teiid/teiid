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
package org.teiid.jboss.rest;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.as.controller.ModelController;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.AdminFactory.AdminImpl;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.ContainerLifeCycleListener;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TransformationMetadata;

public class ResteasyEnabler implements VDBLifeCycleListener {
	static final String REST_NAMESPACE = "{http://teiid.org/rest}"; //$NON-NLS-1$
	private Admin admin;
	private Executor executor;
	private String vdbName;
	private int vdbVersion;
	private AtomicBoolean deployed = new AtomicBoolean(false);
	private ContainerLifeCycleListener shutdownListener;
	
	public ResteasyEnabler(String vdbName, int version, ModelController deployer, Executor executor, ContainerLifeCycleListener shutdownListener) {
		this.admin = AdminFactory.getInstance().createAdmin(deployer.createClient(executor));
		this.executor = executor;
		this.vdbName = vdbName;
		this.vdbVersion = version;
		this.shutdownListener = shutdownListener;
	}
	
	@Override
	public synchronized void added(String name, int version, CompositeVDB vdb, boolean reloading) {
	}
	@Override
	public void beforeRemove(String name, int version, CompositeVDB vdb) {
	}	
	@Override
	public synchronized void finishedDeployment(String name, int version, CompositeVDB cvdb, boolean reloading) {
		if (this.vdbName.equals(name) && this.vdbVersion == version) {

			final VDBMetaData vdb = cvdb.getVDB();
			
			if (!vdb.getStatus().equals(Status.ACTIVE)) {
				return;
			}
			
			String generate = vdb.getPropertyValue(ResteasyEnabler.REST_NAMESPACE+"auto-generate"); //$NON-NLS-1$
	
			final String warName = buildName(vdb);
			if (generate != null && Boolean.parseBoolean(generate)
					&& hasRestMetadata(vdb)
					&& !this.deployed.get()
					&& !reloading) {

				this.deployed.set(true);
				
				final Runnable job = new Runnable() {
					@Override
					public void run() {
						try {
							RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();
							byte[] warContents = builder.createRestArchive(vdb);
							((AdminImpl)admin).deploy(warName, new ByteArrayInputStream(warContents), true);
						} catch (FileNotFoundException e) {
							LogManager.logWarning(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("failed_to_add", warName)); //$NON-NLS-1$
						} catch (IOException e) {
							LogManager.logWarning(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("failed_to_add", warName)); //$NON-NLS-1$;
						} catch (AdminException e) {
							LogManager.logWarning(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("failed_to_add", warName)); //$NON-NLS-1$
						}
					}
				};

				if (!((AdminImpl) admin).getDeployments().contains(warName)) {
					executor.execute(job);
				}
				else {
					// there is timing issue in terms of replacement/re-deploy where remove/add can not be 
					// synchronized correctly. it would have better if there was a way we could inject dependency
					// on war file
					final Timer timer = new Timer("teiid-war-deployer", true); //$NON-NLS-1$
					TimerTask task = new TimerTask() {
						@Override
						public void run() {
							if (!((AdminImpl) admin).getDeployments().contains(warName)) {
								executor.execute(job);
							}
							else {
								LogManager.logWarning(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("failed_to_add", warName)); //$NON-NLS-1$								
							}
						}
					};					
					timer.schedule(task, 3000L);
				}
			}
		}
	}
	
	@Override
	public synchronized void removed(String name, int version, CompositeVDB cvdb) {
		if (this.vdbName.equals(name) && this.vdbVersion == version) {
			VDBMetaData vdb = cvdb.getVDB();
	
			// we only want un-deploy what is auto-generated previously 
			final String warName = buildName(vdb);

			if (this.deployed.get()) {
				this.deployed.set(false);
				if (!this.shutdownListener.isShutdownInProgress()) {
					this.executor.execute(new Runnable() {
						@Override
						public void run() {
							try {
							((AdminImpl)admin).undeploy(warName, true);
							} catch (AdminException e) {
								// during shutdown some times the logging and other subsystems are shutdown, so this operation may not succeed. 
								LogManager.logWarning(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("failed_to_remove", warName)); //$NON-NLS-1$
							}						
						}
					});
				}
			}
		}
	}
	
	private String buildName(VDBMetaData vdb) {
		return vdb.getName().toLowerCase()+"_"+vdb.getVersion()+".war"; //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	private boolean hasRestMetadata(VDBMetaData vdb) {
		String securityType = vdb.getPropertyValue(ResteasyEnabler.REST_NAMESPACE+"security-type"); //$NON-NLS-1$
		if (securityType != null && !securityType.equalsIgnoreCase("none") && !securityType.equalsIgnoreCase("httpbasic")) { //$NON-NLS-1$ //$NON-NLS-2$
			return false;
		}
		
		MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
			Schema schema = metadataStore.getSchema(model.getName());
			if (schema == null) {
				continue; //OTHER type, which does not have a corresponding Teiid schema
			}
			Collection<Procedure> procedures = schema.getProcedures().values();
			for (Procedure procedure:procedures) {
				String uri = procedure.getProperty(REST_NAMESPACE+"URI", false); //$NON-NLS-1$
				String method = procedure.getProperty(REST_NAMESPACE+"METHOD", false); //$NON-NLS-1$
				if (uri != null && method != null) {
					return true;
				}
			}    	
			
		}
		return false;
	}
}
