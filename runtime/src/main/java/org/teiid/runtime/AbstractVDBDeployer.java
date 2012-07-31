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

package org.teiid.runtime;

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.DDLMetadataRepository;
import org.teiid.query.metadata.NativeMetadataRepository;
import org.teiid.translator.TranslatorException;

public abstract class AbstractVDBDeployer {
	
	private Map<String, MetadataRepository<?, ?>> repositories = new ConcurrentSkipListMap<String, MetadataRepository<?, ?>>(String.CASE_INSENSITIVE_ORDER);
	
	public AbstractVDBDeployer() {
		repositories.put("ddl", new DDLMetadataRepository()); //$NON-NLS-1$
		repositories.put("native", new NativeMetadataRepository()); //$NON-NLS-1$
	}
	
	public void addMetadataRepository(String name, MetadataRepository<?, ?> metadataRepository) {
		this.repositories.put(name, metadataRepository);
	}
	
	protected void assignMetadataRepositories(VDBMetaData deployment, MetadataRepository<?, ?> defaultRepo) throws VirtualDatabaseException {
		for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
			if (model.isSource() && model.getSourceNames().isEmpty()) {
	    		throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40093, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40093, model.getName(), deployment.getName(), deployment.getVersion()));
	    	}
			if (model.getModelType() == Type.FUNCTION || model.getModelType() == Type.OTHER) {
				continue;
			}
			MetadataRepository<?, ?> repo = getMetadataRepository(deployment, model, defaultRepo);
			model.addAttchment(MetadataRepository.class, repo);
		}
	}
	
	private MetadataRepository<?, ?> getMetadataRepository(VDBMetaData vdb, ModelMetaData model, MetadataRepository<?, ?> defaultRepo) throws VirtualDatabaseException {
		if (model.getSchemaSourceType() == null) {
			if (!vdb.isDynamic()) {
				return defaultRepo;
			}
			
			if (model.isSource()) {
				return new NativeMetadataRepository();
			}
			throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40094, model.getName(), vdb.getName(), vdb.getVersion(), null));
		}
		
		MetadataRepository<?, ?> first = null;
		MetadataRepository<?, ?> current = null;
		MetadataRepository<?, ?> previous = null;
		StringTokenizer st = new StringTokenizer(model.getSchemaSourceType(), ","); //$NON-NLS-1$
		while (st.hasMoreTokens()) {
			String repoType = st.nextToken().trim();
			current = getMetadataRepository(repoType);
			if (current == null) {
				throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40094, model.getName(), vdb.getName(), vdb.getVersion(), repoType));
			}
		
			if (first == null) {
				first = current;
			}
			
			if (previous != null) {
				previous.setNext(current);
			}
			previous = current;
			current = null;
		}
		return first;
	}
	
    protected ConnectorManager getConnectorManager(final ModelMetaData model, final ConnectorManagerRepository cmr) {
    	if (model.isSource()) {
	    	List<SourceMappingMetadata> mappings = model.getSourceMappings();
			for (SourceMappingMetadata mapping:mappings) {
				return cmr.getConnectorManager(mapping.getName());
			}
    	}
		return null;
    }
    
	protected void loadMetadata(VDBMetaData vdb, ConnectorManagerRepository cmr,
			MetadataStore store) throws TranslatorException {
		// load metadata from the models
		AtomicInteger loadCount = new AtomicInteger();
		for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
			if (model.getModelType() == Model.Type.PHYSICAL || model.getModelType() == Model.Type.VIRTUAL) {
				loadCount.incrementAndGet();
			}
		}
		for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
			MetadataRepository metadataRepository = model.getAttachment(MetadataRepository.class);
			if (model.getModelType() == Model.Type.PHYSICAL || model.getModelType() == Model.Type.VIRTUAL) {
				loadMetadata(vdb, model, cmr, metadataRepository, store, loadCount);
				LogManager.logTrace(LogConstants.CTX_RUNTIME, "Model ", model.getName(), "in VDB ", vdb.getName(), " was being loaded from its repository"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
			else {
				LogManager.logTrace(LogConstants.CTX_RUNTIME, "Model ", model.getName(), "in VDB ", vdb.getName(), " skipped being loaded because of its type ", model.getModelType()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
		}
	}
	
	protected abstract VDBRepository getVDBRepository();

	protected abstract void loadMetadata(VDBMetaData vdb, ModelMetaData model,
			ConnectorManagerRepository cmr,
			MetadataRepository metadataRepository, MetadataStore store,
			AtomicInteger loadCount) throws TranslatorException;
	
	protected void metadataLoaded(final VDBMetaData vdb,
			final ModelMetaData model,
			final MetadataStore vdbMetadataStore,
			final AtomicInteger loadCount, MetadataFactory factory, boolean success) {
		if (success) {
			// merge into VDB metadata
			factory.mergeInto(vdbMetadataStore);
		
			//TODO: this is not quite correct, the source may be missing
			model.clearRuntimeMessages();
		} else {
			vdb.setStatus(Status.FAILED);
			//TODO: abort the other loads
		}
		
		if (loadCount.decrementAndGet() == 0 || vdb.getStatus() == Status.FAILED) {
			getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion());
		}
	}

	/**
	 * @throws VirtualDatabaseException  
	 */
	protected MetadataRepository<?, ?> getMetadataRepository(String repoType) throws VirtualDatabaseException {
		return repositories.get(repoType);
	}
}
