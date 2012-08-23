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

import org.jboss.as.controller.ModelController;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.AdminFactory.AdminImpl;
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

public class ResteasyEnabler implements VDBLifeCycleListener {
	static final String REST_NAMESPACE = "{http://teiid.org/rest}"; //$NON-NLS-1$
	private Admin admin;
	private Executor executor;
	
	public ResteasyEnabler(ModelController deployer, Executor executor) {
		this.admin = AdminFactory.getInstance().createAdmin(deployer.createClient(executor));
		this.executor = executor;
	}
	
	@Override
	public void added(String name, int version, CompositeVDB vdb) {
	}
	
	@Override
	public void finishedDeployment(String name, int version, CompositeVDB cvdb) {
		final VDBMetaData vdb = cvdb.getVDB();
		
		String generate = vdb.getPropertyValue("auto-generate-rest-war"); //$NON-NLS-1$

		final String warName = buildName(vdb);
		if (generate != null && Boolean.parseBoolean(generate)
				&& hasRestMetadata(vdb)
				&& !((AdminImpl) this.admin).getDeployments().contains(warName)) {
			// this must be executing the async thread to avoid any lock-up from management operations
			this.executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();
						byte[] warContents = builder.createRestArchive(vdb);
						admin.deploy(warName, new ByteArrayInputStream(warContents));
					} catch (FileNotFoundException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e);
					} catch (IOException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e);
					} catch (AdminException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e);
					}
				}
			});
		}
	}
	
	@Override
	public void removed(String name, int version, CompositeVDB cvdb) {
		VDBMetaData vdb = cvdb.getVDB();
		String generate = vdb.getPropertyValue("auto-generate-rest-war"); //$NON-NLS-1$
		final String warName = buildName(vdb);
		if (generate != null && Boolean.parseBoolean(generate)
				&& ((AdminImpl) this.admin).getDeployments().contains(warName)) {
			this.executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
					admin.undeploy(warName);
					} catch (AdminException e) {
						LogManager.logWarning(LogConstants.CTX_RUNTIME, e);
					}						
				}
			});
		}
	}
	
	private String buildName(VDBMetaData vdb) {
		return vdb.getName().toLowerCase()+"_"+vdb.getVersion()+".war"; //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	private boolean hasRestMetadata(VDBMetaData vdb) {
		MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
			Schema schema = metadataStore.getSchema(model.getName());
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
