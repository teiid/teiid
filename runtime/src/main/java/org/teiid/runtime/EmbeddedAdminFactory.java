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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.CacheStatistics;
import org.teiid.adminapi.EngineStatistics;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.WorkerPoolStatistics;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.EngineStatisticsMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.PropertyDefinitionMetadata;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.deployers.ExtendedPropertyMetadata;
import org.teiid.deployers.ExtendedPropertyMetadataList;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.services.BufferServiceImpl;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorProperty.PropertyType;

public class EmbeddedAdminFactory {
	
	private static EmbeddedAdminFactory INSTANCE = new EmbeddedAdminFactory();
	
	private EmbeddedAdminFactory(){
		
	}

	public static EmbeddedAdminFactory getInstance() {
		return INSTANCE;
	}
	
	public static EngineStatisticsMetadata createEngineStats(
			int activeSessionsCount, BufferServiceImpl bufferService,
			DQPCore dqp) {
		EngineStatisticsMetadata stats = new EngineStatisticsMetadata();
		stats.setSessionCount(activeSessionsCount);
		stats.setTotalMemoryUsedInKB(bufferService.getHeapCacheMemoryInUseKB());
		stats.setMemoryUsedByActivePlansInKB(bufferService.getHeapMemoryInUseByActivePlansKB());
		stats.setDiskWriteCount(bufferService.getDiskWriteCount());
		stats.setDiskReadCount(bufferService.getDiskReadCount());
		stats.setCacheReadCount(bufferService.getCacheReadCount());
		stats.setCacheWriteCount(bufferService.getCacheWriteCount());
		stats.setDiskSpaceUsedInMB(bufferService.getUsedDiskBufferSpaceMB());
		stats.setActivePlanCount(dqp.getActivePlanCount());
		stats.setWaitPlanCount(dqp.getWaitingPlanCount());
		stats.setMaxWaitPlanWaterMark(dqp.getMaxWaitingPlanWatermark());
		return stats;
	}

	Admin createAdmin(EmbeddedServer embeddedServer) {
		return new AdminImpl(embeddedServer);
	}
	
	private static class AdminImpl implements Admin {
		
		private EmbeddedServer embeddedServer;

		public AdminImpl(EmbeddedServer embeddedServer) {
			this.embeddedServer = embeddedServer;
		}
		
		@Override
		public void assignToModel(String vdbName, int vdbVersion, String modelName, String sourceName, String translatorName, String dsName) throws AdminException {
			// assignToModel is Deprecated
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40130, "assignToModel")); //$NON-NLS-1$
			
		}

		@Override
		public void removeSource(String vdbName, int vdbVersion, String modelName, String sourceName) throws AdminException {

			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				ModelMetaData model = vdb.getModel(modelName);
				
				if (model == null) {
					 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40090, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40090, modelName, vdb.getName(), vdb.getVersion()));
				}
				
				SourceMappingMetadata source = model.getSourceMapping(sourceName);
				if(source == null) {
					 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40107, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40107, sourceName, modelName, vdb.getName(), vdb.getVersion()));
				}
				
				source = model.getSources().remove(sourceName);
				if(source == null) {
					 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40091, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40091, sourceName, modelName, vdb.getName(), vdb.getVersion()));
				}
			}
			
		}
		
		private VDBMetaData checkVDB(String vdbName) throws AdminProcessingException {
			VDBMetaData vdb = this.embeddedServer.repo.getLiveVDB(vdbName);
			return vdb;
		}
		
		private VDBMetaData checkVDB(String vdbName, int vdbVersion) throws AdminProcessingException {
			VDBMetaData vdb = this.embeddedServer.repo.getLiveVDB(vdbName, vdbVersion);
			if (vdb == null){
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40131, vdbName, vdbVersion));
			}
			Status status = vdb.getStatus();
			if (status != VDB.Status.ACTIVE) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40132, vdbName, vdbVersion, status));
			}
			return vdb;
		}

		@Override
		public void addSource(String vdbName, int vdbVersion, String modelName, String sourceName, String translatorName, String dsName) throws AdminException {
			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				ModelMetaData model = vdb.getModel(modelName);
				
				if (model == null) {
					 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40090, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40090, modelName, vdb.getName(), vdb.getVersion()));
				}
				
				if (!model.isSupportsMultiSourceBindings()) {
					throw new AdminProcessingException(RuntimePlugin.Event.TEIID40108, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40108, modelName, vdb.getName(), vdb.getVersion()));
				}
				
				SourceMappingMetadata source = model.getSourceMapping(sourceName);
				if(source != null) {
					 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40107, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40107, sourceName, modelName, vdb.getName(), vdb.getVersion()));
				}

				SourceMappingMetadata mapping = new SourceMappingMetadata(sourceName, translatorName, dsName);
				model.addSourceMapping(mapping);
			}
		}

		@Override
		public void updateSource(String vdbName, int vdbVersion, String sourceName, String translatorName, String dsName) throws AdminException {
			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				
				for (ModelMetaData m : vdb.getModelMetaDatas().values()) {
					SourceMappingMetadata mapping = m.getSourceMapping(sourceName);
					if (mapping != null) {
						mapping.setTranslatorName(translatorName);
						mapping.setConnectionJndiName(dsName);
					}
				}
			}
		}
		
		@Override
		public void changeVDBConnectionType(String vdbName, int vdbVersion, ConnectionType type) throws AdminException {
			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				vdb.setConnectionType(type);
			}
		}

		@Override
		public void deploy(String deployName, InputStream content) throws AdminException {
			if (!deployName.endsWith("-vdb.xml")) { //$NON-NLS-1$
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40142, deployName)); 
			}
			try {
				this.embeddedServer.deployVDB(content);
			} catch (Exception e) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40133, deployName, e), e);
			}
			//non xml would be inelegant for embedded - we don't want to create an intermediate copy
            /*File f = null;
			try {
				f = File.createTempFile(deployName, "vdb"); //$NON-NLS-1$
				f.deleteOnExit();
				ObjectConverterUtil.write(content, f);
				this.embeddedServer.deployVDBZip(f.toURI().toURL());
			} catch (Exception e) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40133, deployName, e), e);
			}*/ 
		}

		@Override
		public void undeploy(String deployedName) throws AdminException {
			
			VDBMetaData vdb = checkVDB(deployedName);
			if(null == vdb) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40134, deployedName));
			}
			
			this.embeddedServer.undeployVDB(deployedName);
		}

		@Override
		public Collection<? extends VDB> getVDBs() throws AdminException {
			List<VDB> list = new ArrayList<VDB>();
			list.addAll(this.embeddedServer.repo.getVDBs());
			return list;
		}

		@Override
		public VDB getVDB(String vdbName, int vdbVersion) throws AdminException {
			return this.embeddedServer.repo.getVDB(vdbName, vdbVersion);
		}

		@Override
		public void restartVDB(String vdbName, int vdbVersion, String... models) throws AdminException {

			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				try {
					// need remove model cache first
					VDBMetaData currentVdb = this.embeddedServer.repo.removeVDB(vdbName, vdbVersion);
					ConnectorManagerRepository cmr = this.embeddedServer.cmr;
					UDFMetaData udf = currentVdb.getAttachment(UDFMetaData.class);
					// need get the visibilityMap from runtime
					LinkedHashMap<String, VDBResources.Resource> visibilityMap = new LinkedHashMap<String, VDBResources.Resource>();
					MetadataStore store = new MetadataStore();
					this.embeddedServer.repo.addVDB(currentVdb, store, visibilityMap, udf, cmr, false);
				} catch (Exception e) {
					throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40135, vdbName, vdbVersion, models), e);
				}
			}
			
		}

		@Override
		public Collection<? extends org.teiid.adminapi.Translator> getTranslators() throws AdminException {
			
			List<org.teiid.adminapi.Translator> list = new ArrayList<org.teiid.adminapi.Translator>();
			for(ExecutionFactory<?,?> ef : this.embeddedServer.getTranslators().values()){
				list.add(TranslatorUtil.buildTranslatorMetadata(ef, null));
			}
			return list;
		}

		@Override
		public org.teiid.adminapi.Translator getTranslator(String deployedName) throws AdminException {
			
			try {
				ExecutionFactory<?,?> ef = this.embeddedServer.getExecutionFactory(deployedName);
				VDBTranslatorMetaData metadata = TranslatorUtil.buildTranslatorMetadata(ef, null);
				return metadata;
			} catch (ConnectorManagerException e) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40136, deployedName, e));
			}
		}

		@Override
		public Collection<? extends WorkerPoolStatistics> getWorkerPoolStats() throws AdminException {
			return Arrays.asList(this.embeddedServer.dqp.getWorkerPoolStatistics());
		}

		@Override
		public Collection<String> getCacheTypes() throws AdminException {
			Set<String> cacheTypes = new HashSet<String>();
			cacheTypes.addAll(SessionAwareCache.getCacheTypes());
			return cacheTypes;
		}

		@Override
		public Collection<? extends Session> getSessions() throws AdminException {
			List<Session> list = new ArrayList<Session>();
			list.addAll(this.embeddedServer.sessionService.getActiveSessions());
			return list;
		}

		@Override
		public Collection<? extends Request> getRequests() throws AdminException {
			List<Request> list = new ArrayList<Request>();
			list.addAll(this.embeddedServer.dqp.getRequests());
			return list;
		}

		@Override
		public Collection<? extends Request> getRequestsForSession(String sessionId) throws AdminException {
			List<Request> list = new ArrayList<Request>();
			list.addAll(this.embeddedServer.dqp.getRequestsForSession(sessionId));
			return list;
		}

		@Override
		public Collection<? extends PropertyDefinition> getTemplatePropertyDefinitions(String templateName) throws AdminException {
			// Embedded dosn't load ra.xml
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "getTemplatePropertyDefinitions")); //$NON-NLS-1$
		}

		@Override
		public Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(String translatorName) throws AdminException {
			//deprecated
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40130, "getTranslatorPropertyDefinitions")); //$NON-NLS-1$
		}

		@Override
		public Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(String translatorName, TranlatorPropertyType type) throws AdminException {
			
			List<PropertyDefinition> list = new ArrayList<PropertyDefinition>();
			try {
				ExecutionFactory<?,?> ef = this.embeddedServer.getExecutionFactory(translatorName);
				if (ef == null) {
					return null;
				}
				VDBTranslatorMetaData translator = TranslatorUtil.buildTranslatorMetadata(ef, null);
				TranlatorPropertyType translatorPropertyType = TranlatorPropertyType.valueOf(type.toString().toUpperCase());
				if (translator != null) {
					ExtendedPropertyMetadataList properties = translator.getAttachment(ExtendedPropertyMetadataList.class);					
		            if (translatorPropertyType.equals(TranlatorPropertyType.ALL)) {
		                for (ExtendedPropertyMetadata epm:properties) {
                            list.add(buildNode(epm));
		                }
		            } else {
		                PropertyType propType = PropertyType.valueOf(type.toString().toUpperCase());
	                    for (ExtendedPropertyMetadata epm:properties) {
	                        if (PropertyType.valueOf(epm.category()).equals(propType)) {
	                            list.add(buildNode(epm));
	                        }
	                    }    
		            }
				}
			} catch (ConnectorManagerException e) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40138, translatorName, type, e));
			}
			return list;
		}

        private PropertyDefinitionMetadata buildNode(
                ExtendedPropertyMetadata epm) {
            PropertyDefinitionMetadata pdm = new PropertyDefinitionMetadata();
            pdm.setAdvanced(epm.advanced() );
            pdm.setAllowedValues(Arrays.asList(epm.allowed()));
            pdm.setDefaultValue(epm.defaultValue());
            pdm.setDescription(epm.description());
            pdm.setDisplayName(epm.display());
            pdm.setMasked(epm.masked());
            pdm.setName(epm.name());
            pdm.setPropertyTypeClassName(epm.datatype());
            if(epm.readOnly()){
            	pdm.setModifiable(false);
            } else {
            	pdm.setModifiable(true);
            }
            pdm.setRequired(epm.required());
            pdm.setCategory(epm.category());
            return pdm;
        }

		@Override
		public Collection<? extends Transaction> getTransactions() throws AdminException {
			return this.embeddedServer.dqp.getTransactions();
		}

		@Override
		public void clearCache(String cacheType) throws AdminException {
			
			if(cacheType.equals(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name())){
				this.embeddedServer.getRsCache().clearAll();
			} else if(cacheType.equals(Admin.Cache.PREPARED_PLAN_CACHE.name())) {
				this.embeddedServer.getPpcCache().clearAll();
			} else {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40139, cacheType, Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE, Admin.Cache.PREPARED_PLAN_CACHE));
			}
		}

		@Override
		public void clearCache(String cacheType, String vdbName, int vdbVersion) throws AdminException {
			
			checkVDB(vdbName, vdbVersion);

			if(cacheType.equals(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name())){
				this.embeddedServer.getRsCache().clearForVDB(vdbName, vdbVersion);
			} else if(cacheType.equals(Admin.Cache.PREPARED_PLAN_CACHE.name())) {
				this.embeddedServer.getPpcCache().clearForVDB(vdbName, vdbVersion);
			} else {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40139, cacheType, Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE, Admin.Cache.PREPARED_PLAN_CACHE));
			}
		}

		@Override
		public Collection<? extends CacheStatistics> getCacheStats(String cacheType) throws AdminException {
			
			if(cacheType.equals(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name())){
				return Arrays.asList(this.embeddedServer.getRsCache().buildCacheStats(cacheType));
			} else if(cacheType.equals(Admin.Cache.PREPARED_PLAN_CACHE.name())) {
				return Arrays.asList(this.embeddedServer.getPpcCache().buildCacheStats(cacheType));
			} else {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40139, cacheType, Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE, Admin.Cache.PREPARED_PLAN_CACHE));
			}				
		}
		
		@Override
		public Collection<? extends EngineStatistics> getEngineStats() throws AdminException {
			
			try {
				//embedded no logon, odata, odbc 
				EngineStatisticsMetadata stats = createEngineStats(
						this.embeddedServer.sessionService.getActiveSessionsCount(), this.embeddedServer.bufferService, this.embeddedServer.dqp);
				return Arrays.asList(stats);
			} catch (SessionServiceException e) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40140, "getEngineStats", e)); //$NON-NLS-1$
			}
			
		}

		@Override
		public void terminateSession(String sessionId)throws AdminException {
			this.embeddedServer.sessionService.terminateSession(sessionId, DQPWorkContext.getWorkContext().getSessionId());
		}

		@Override
		public void cancelRequest(String sessionId, long executionId)throws AdminException {
			try {
				this.embeddedServer.dqp.cancelRequest(sessionId, executionId);
			} catch (TeiidComponentException e) {
				throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40141, sessionId, executionId, e));
			}
		}

		@Override
		public void terminateTransaction(String transactionId)throws AdminException {
			this.embeddedServer.dqp.terminateTransaction(transactionId);
		}

		@Override
		public void close() {
			//nothing to do
		}

		@Override
		public void addDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException {
			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				DataPolicyMetadata policy = getPolicy(vdb, dataRole);
				policy.addMappedRoleName(mappedRoleName);			
			}
		}
		
		private DataPolicyMetadata getPolicy(VDBMetaData vdb, String policyName)throws AdminProcessingException {
			DataPolicyMetadata policy = vdb.getDataPolicyMap().get(policyName);	
			if (policy == null) {
				 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40092, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40092, policyName, vdb.getName(), vdb.getVersion()));
			}
			return policy;
		}	

		@Override
		public void removeDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException {
			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				DataPolicyMetadata policy = getPolicy(vdb, dataRole);
				policy.removeMappedRoleName(mappedRoleName);
			}
		}

		@Override
		public void setAnyAuthenticatedForDataRole(String vdbName, int vdbVersion, String dataRole, boolean anyAuthenticated) throws AdminException {
			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			synchronized(vdb) {
				DataPolicyMetadata policy = getPolicy(vdb, dataRole);
				policy.setAnyAuthenticated(anyAuthenticated);
			}
		}

		@Override
		public void createDataSource(String deploymentName, String templateName, Properties properties) throws AdminException {
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "createDataSource")); //$NON-NLS-1$
		}

		@Override
		public Properties getDataSource(String deployedName) throws AdminException {
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "getDataSource")); //$NON-NLS-1$
		}

		@Override
		public void deleteDataSource(String deployedName) throws AdminException {
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "deleteDataSource")); //$NON-NLS-1$
		}

		@Override
		public Collection<String> getDataSourceNames() throws AdminException {
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "getDataSourceNames")); //$NON-NLS-1$
		}

		@Override
		public Set<String> getDataSourceTemplateNames() throws AdminException {
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "getDataSourceTemplateNames")); //$NON-NLS-1$
		}

		@Override
		public void markDataSourceAvailable(String jndiName) throws AdminException {
			throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "markDataSourceAvailable")); //$NON-NLS-1$
		}

		@Override
		public String getSchema(String vdbName, int vdbVersion, String modelName, EnumSet<SchemaObjectType> allowedTypes, String typeNamePattern) throws AdminException {
			
			VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
			
			MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
			Schema schema = metadataStore.getSchema(modelName);
			String ddl = DDLStringVisitor.getDDLString(schema, allowedTypes, typeNamePattern);
			return ddl;
		}

		@Override
		public String getQueryPlan(String sessionId, int executionId)throws AdminException {
			PlanNode plan = this.embeddedServer.dqp.getPlan(sessionId, executionId);
			if (plan == null) {
				return null;
			}
			return plan.toXml();
		}

		@Override
		public void restart() {
			// embedded once stop can not restart
			throw new RuntimeException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "restart")); //$NON-NLS-1$
		}
		
	}

}
