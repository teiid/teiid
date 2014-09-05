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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.teiid.core.CoreConstants;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.VDBResource;
import org.teiid.query.metadata.ChainingMetadataRepository;
import org.teiid.query.metadata.DDLFileMetadataRepository;
import org.teiid.query.metadata.DDLMetadataRepository;
import org.teiid.query.metadata.DirectQueryMetadataRepository;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.NativeMetadataRepository;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.parser.QueryParser;
import org.teiid.translator.TranslatorException;

public abstract class AbstractVDBDeployer {
	
	protected ConcurrentSkipListMap<String, MetadataRepository<?, ?>> repositories = new ConcurrentSkipListMap<String, MetadataRepository<?, ?>>(String.CASE_INSENSITIVE_ORDER);
	
	public AbstractVDBDeployer() {
		repositories.put("ddl", new DDLMetadataRepository()); //$NON-NLS-1$
		repositories.put("native", new NativeMetadataRepository()); //$NON-NLS-1$
		repositories.put("ddl-file", new DDLFileMetadataRepository()); //$NON-NLS-1$
	}
	
	public void addMetadataRepository(String name, MetadataRepository<?, ?> metadataRepository) {
		this.repositories.put(name, metadataRepository);
	}
	
	protected void assignMetadataRepositories(VDBMetaData deployment, MetadataRepository<?, ?> defaultRepo) throws VirtualDatabaseException {
		for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
			if (model.getModelType() != Type.OTHER && (model.getName() == null || model.getName().indexOf('.') >= 0) 
					|| model.getName().equalsIgnoreCase(CoreConstants.SYSTEM_MODEL)
					|| model.getName().equalsIgnoreCase(CoreConstants.SYSTEM_ADMIN_MODEL)
					|| model.getName().equalsIgnoreCase(CoreConstants.ODBC_MODEL)) {
				throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40121, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40121, model.getName(), deployment.getName(), deployment.getVersion()));
			}
			if (model.isSource() && model.getSourceNames().isEmpty()) {
	    		throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40093, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40093, model.getName(), deployment.getName(), deployment.getVersion()));
	    	}
			if (model.getModelType() == Type.FUNCTION || model.getModelType() == Type.OTHER) {
				continue;
			}
			MetadataRepository<?, ?> repo = getMetadataRepository(deployment, model, defaultRepo);
			//handle multi-source column creation
			if (model.isSupportsMultiSourceBindings() && Boolean.valueOf(model.getPropertyValue("multisource.addColumn"))) { //$NON-NLS-1$
				List<MetadataRepository<?, ?>> repos = new ArrayList<MetadataRepository<?, ?>>(2);
				repos.add(repo);
				String columnName = model.getPropertyValue(MultiSourceMetadataWrapper.MULTISOURCE_COLUMN_NAME);
				repos.add(new MultiSourceMetadataRepository(columnName==null?MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME:columnName));
				repo = new ChainingMetadataRepository(repos);
			}
			model.addAttchment(MetadataRepository.class, repo);
		}
	}
	
	private MetadataRepository<?, ?> getMetadataRepository(VDBMetaData vdb, ModelMetaData model, MetadataRepository<?, ?> defaultRepo) throws VirtualDatabaseException {
		if (model.getSchemaSourceType() == null) {
			if (defaultRepo != null) {
				return defaultRepo;
			}
			if (model.isSource()) {
				return new ChainingMetadataRepository(Arrays.asList(new NativeMetadataRepository(), new DirectQueryMetadataRepository()));
			}
			throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40094, model.getName(), vdb.getName(), vdb.getVersion(), null));
		}
		String schemaTypes = model.getSchemaSourceType();
		StringTokenizer st = new StringTokenizer(schemaTypes, ","); //$NON-NLS-1$
		List<MetadataRepository<?, ?>> repos = new ArrayList<MetadataRepository<?,?>>(st.countTokens());
		while (st.hasMoreTokens()) {
			String repoType = st.nextToken().trim();
			MetadataRepository<?, ?> current = getMetadataRepository(repoType);
			if (current == null) {
				throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40094, model.getName(), vdb.getName(), vdb.getVersion(), repoType));
			}
			repos.add(current);
		}
		if (model.getModelType() == ModelMetaData.Type.PHYSICAL) {
			repos.add(new DirectQueryMetadataRepository());
		}
		if (model.getModelType() == ModelMetaData.Type.VIRTUAL) {
			repos.add(new MaterializationMetadataRepository());
		}
		if (repos.size() == 1) {
			return repos.get(0);
		}
		return new ChainingMetadataRepository(repos);
	}
	
    protected List<ConnectorManager> getConnectorManagers(final ModelMetaData model, final ConnectorManagerRepository cmr) {
    	if (model.isSource()) {
        	Collection<SourceMappingMetadata> mappings = model.getSources().values();
    		List<ConnectorManager> result = new ArrayList<ConnectorManager>(mappings.size());
			for (SourceMappingMetadata mapping:mappings) {
				result.add(cmr.getConnectorManager(mapping.getName()));
			}
			return result;
    	}
    	//return a single null to give us something to loop over
		return Collections.singletonList(null);
    }
    
	protected void loadMetadata(VDBMetaData vdb, ConnectorManagerRepository cmr,
			MetadataStore store, VDBResources vdbResources, boolean reloading) throws TranslatorException {
		// load metadata from the models
		AtomicInteger loadCount = new AtomicInteger();
		for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
			if (model.getModelType() == Model.Type.PHYSICAL || model.getModelType() == Model.Type.VIRTUAL) {
				loadCount.incrementAndGet();
			}
		}
		if (loadCount.get() == 0) {
			getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion(), reloading);
			return;
		}
		for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
			MetadataRepository metadataRepository = model.getAttachment(MetadataRepository.class);
			if (model.getModelType() == Model.Type.PHYSICAL || model.getModelType() == Model.Type.VIRTUAL) {
				loadMetadata(vdb, model, cmr, metadataRepository, store, loadCount, vdbResources);
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
			AtomicInteger loadCount, VDBResources vdbResources) throws TranslatorException;
	
	protected void metadataLoaded(final VDBMetaData vdb,
			final ModelMetaData model,
			final MetadataStore vdbMetadataStore,
			final AtomicInteger loadCount, MetadataFactory factory, boolean success, boolean reloading) {
		if (success) {
			// merge into VDB metadata
			factory.mergeInto(vdbMetadataStore);
		
			//TODO: this is not quite correct, the source may be missing
			model.clearRuntimeMessages();
			model.setMetadataStatus(Model.MetadataStatus.LOADED);
		} else {
			model.setMetadataStatus(Model.MetadataStatus.FAILED);
			vdb.setStatus(Status.FAILED);
			//TODO: abort the other loads
		}
		
		if (loadCount.decrementAndGet() == 0 || vdb.getStatus() == Status.FAILED) {
			getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion(), reloading);
		}
	}
	
	protected MetadataFactory createMetadataFactory(VDBMetaData vdb,
			ModelMetaData model, Map<String, ? extends VDBResource> vdbResources) {
		Map<String, Datatype> datatypes = this.getVDBRepository().getRuntimeTypeMap();
		MetadataFactory factory = new MetadataFactory(vdb.getName(), vdb.getVersion(), datatypes, model);
		factory.setBuiltinDataTypes(this.getVDBRepository().getSystemStore().getDatatypes());
		factory.getSchema().setPhysical(model.isSource());
		factory.setParser(new QueryParser()); //for thread safety each factory gets it's own instance.
		factory.setVdbResources(vdbResources);
		return factory;
	}

	/**
	 * @throws VirtualDatabaseException  
	 */
	protected MetadataRepository<?, ?> getMetadataRepository(String repoType) throws VirtualDatabaseException {
		return repositories.get(repoType);
	}
}
