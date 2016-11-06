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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TransformationMetadata;
/*
        Admin admin = AdminFactory.getInstance().createAdmin(controllerValue
                .getValue().createClient(executorInjector.getValue()));     
        this.restEasyListener = new ResteasyEnabler(this.vdb.getName(),
                this.vdb.getVersion(), admin, executorInjector.getValue(),
                shutdownListener);
        getVDBRepository().addListener(this.restEasyListener);  
 */
public class ResteasyEnabler implements VDBLifeCycleListener {
	static final String REST_NAMESPACE = "{http://teiid.org/rest}"; //$NON-NLS-1$
	private Admin admin;
	private Executor executor;
	private AtomicBoolean deployed = new AtomicBoolean(false);
	
	public ResteasyEnabler(Admin admin, Executor executor) {
		this.admin = admin;
		this.executor = executor;
	}
	
	@Override
	public synchronized void added(String name, CompositeVDB vdb) {
	}
	
	@Override
	public void beforeRemove(String name, CompositeVDB cvdb) {
    	this.deployed.set(false);
	}	
	
	@Override
	public synchronized void finishedDeployment(String name, CompositeVDB cvdb) {
		final VDBMetaData vdb = cvdb.getVDB();
		
		if (!vdb.getStatus().equals(Status.ACTIVE)) {
			return;
		}
		
		String generate = vdb.getPropertyValue(ResteasyEnabler.REST_NAMESPACE+"auto-generate"); //$NON-NLS-1$

		final String warName = buildName(name, cvdb.getVDB().getVersion());
		if (generate != null && Boolean.parseBoolean(generate)
				&& hasRestMetadata(vdb)
				&& this.deployed.compareAndSet(false, true)) {

			final Runnable job = new Runnable() {
				@Override
				public void run() {
					try {
						RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();
						byte[] warContents = builder.createRestArchive(vdb);
						if (!vdb.getStatus().equals(Status.ACTIVE)) {
							return;
						}
						//make it a non-persistent deployment
						admin.deploy(warName, new ByteArrayInputStream(warContents), false);
					} catch (FileNotFoundException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e, 
						        RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28004, warName)); 
					} catch (IOException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e, 
						        RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28004, warName)); 
					} catch (AdminException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e, 
						        RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28004, warName)); 
					}
				}
			};

			executor.execute(job);
		}
	}
	
	@Override
	public synchronized void removed(String name, CompositeVDB cvdb) {

	}
	
	private String buildName(String name, String version) {
		return name+"_"+version +".war"; //$NON-NLS-1$ //$NON-NLS-2$
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
