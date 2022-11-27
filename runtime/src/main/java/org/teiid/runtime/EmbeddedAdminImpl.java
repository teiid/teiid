package org.teiid.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.teiid.adminapi.*;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
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
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.metadata.Database;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.vdb.runtime.VDBKey;

public class EmbeddedAdminImpl implements Admin {

    private EmbeddedServer embeddedServer;

    public EmbeddedAdminImpl(EmbeddedServer embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    @Override
    public void setProfileName(String name) {
        //no op
    }

    @Override
    public void clearCache(String cacheType, String vdbName, int vdbVersion)
            throws AdminException {
        clearCache(cacheType, vdbName, String.valueOf(vdbVersion));
    }

    @Override
    public void addDataRoleMapping(String vdbName, int vdbVersion,
            String dataRole, String mappedRoleName) throws AdminException {
        addDataRoleMapping(vdbName, String.valueOf(vdbVersion), dataRole, mappedRoleName);
    }

    @Override
    public void removeDataRoleMapping(String vdbName, int vdbVersion,
            String dataRole, String mappedRoleName) throws AdminException {
        removeDataRoleMapping(vdbName, String.valueOf(vdbVersion), dataRole, mappedRoleName);
    }

    @Override
    public void setAnyAuthenticatedForDataRole(String vdbName,
            int vdbVersion, String dataRole, boolean anyAuthenticated)
            throws AdminException {
        setAnyAuthenticatedForDataRole(vdbName, String.valueOf(vdbVersion), dataRole, anyAuthenticated);
    }

    @Override
    public void changeVDBConnectionType(String vdbName, int vdbVersion,
            ConnectionType type) throws AdminException {
        changeVDBConnectionType(vdbName, String.valueOf(vdbVersion), type);
    }

    @Override
    public void updateSource(String vdbName, int vdbVersion,
            String sourceName, String translatorName, String dsName)
            throws AdminException {
        updateSource(vdbName, String.valueOf(vdbVersion), sourceName, translatorName, dsName);
    }

    @Override
    public void addSource(String vdbName, int vdbVersion, String modelName,
            String sourceName, String translatorName, String dsName)
            throws AdminException {
        addSource(vdbName, String.valueOf(vdbVersion), modelName, sourceName, translatorName, dsName);
    }

    @Override
    public VDB getVDB(String vdbName, int vdbVersion) throws AdminException {
        return getVDB(vdbName, String.valueOf(vdbVersion));
    }

    @Override
    public void removeSource(String vdbName, int vdbVersion,
            String modelName, String sourceName) throws AdminException {
        removeSource(vdbName, String.valueOf(vdbVersion), modelName, sourceName);
    }

    @Override
    public void restartVDB(String vdbName, int vdbVersion, String... models)
            throws AdminException {
        restartVDB(vdbName, String.valueOf(vdbVersion), models);
    }

    @Override
    public String getSchema(String vdbName, int vdbVersion,
            String modelName, EnumSet<SchemaObjectType> allowedTypes,
            String typeNamePattern) throws AdminException {
        return getSchema(vdbName, String.valueOf(vdbVersion), modelName, allowedTypes, typeNamePattern);
    }

    @Override
    public void removeSource(String vdbName, String vdbVersion, String modelName, String sourceName) throws AdminException {

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

    private VDBMetaData checkVDB(String vdbName) {
        VDBMetaData vdb = this.embeddedServer.repo.getLiveVDB(vdbName);
        return vdb;
    }

    private VDBMetaData checkVDB(String vdbName, String vdbVersion) throws AdminProcessingException {
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
    public void addSource(String vdbName, String vdbVersion, String modelName, String sourceName, String translatorName, String dsName) throws AdminException {
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
    public void updateSource(String vdbName, String vdbVersion, String sourceName, String translatorName, String dsName) throws AdminException {
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
    public void changeVDBConnectionType(String vdbName, String vdbVersion, ConnectionType type) throws AdminException {
        VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
        synchronized(vdb) {
            vdb.setConnectionType(type);
        }
    }

    @Override
    public void deployVDBZip(URL url) throws AdminProcessingException {
        try {
            this.embeddedServer.deployVDBZip(url);
        } catch (VirtualDatabaseException | ConnectorManagerException
                | TranslatorException | IOException | URISyntaxException e) {
            throw new AdminProcessingException(e);
        }
    }

    @Override
    public void deploy(String deployName, InputStream content) throws AdminException {
        if (!deployName.endsWith("-vdb.xml") && !deployName.endsWith("-vdb.ddl")) { //$NON-NLS-1$ //$NON-NLS-2$
            throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40142, deployName));
        }
        try {
            this.embeddedServer.deployVDB(content, deployName.endsWith("-vdb.ddl")); //$NON-NLS-1$
        } catch (Exception e) {
            throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40133, deployName, e), e);
        }
    }

    @Override
    public void deploy(String deployName, InputStream content, boolean peristent) throws AdminException {
        throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "deploy")); //$NON-NLS-1$
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
    public Collection<? extends VDB> getVDBs(boolean singleInstance)
            throws AdminException {
        return getVDBs();
    }

    @Override
    public VDB getVDB(String vdbName, String vdbVersion) throws AdminException {
        return this.embeddedServer.repo.getVDB(vdbName, vdbVersion);
    }

    @Override
    public void restartVDB(String vdbName, String vdbVersion, String... models) throws AdminException {

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
                this.embeddedServer.repo.addVDB(currentVdb, store, visibilityMap, udf, cmr);
            } catch (Exception e) {
                throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40135, vdbName, vdbVersion, models), e);
            }
        }

    }

    @Override
    public Collection<? extends org.teiid.adminapi.Translator> getTranslators() throws AdminException {
        return this.embeddedServer.getTranslatorRepository().getTranslators();
    }

    @Override
    public org.teiid.adminapi.Translator getTranslator(String deployedName) throws AdminException {
        return this.embeddedServer.getTranslatorRepository().getTranslatorMetaData(deployedName);
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
        VDBTranslatorMetaData translator = this.embeddedServer.getTranslatorRepository().getTranslatorMetaData(translatorName);
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
    public void clearCache(String cacheType, String vdbName, String vdbVersion) throws AdminException {

        checkVDB(vdbName, vdbVersion);

        if(cacheType.equals(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name())){
            this.embeddedServer.getRsCache().clearForVDB(new VDBKey(vdbName, vdbVersion));
        } else if(cacheType.equals(Admin.Cache.PREPARED_PLAN_CACHE.name())) {
            this.embeddedServer.getPpcCache().clearForVDB(new VDBKey(vdbName, vdbVersion));
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
            EngineStatisticsMetadata stats = EmbeddedAdminFactory.createEngineStats(
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
    public void addDataRoleMapping(String vdbName, String vdbVersion, String dataRole, String mappedRoleName) throws AdminException {
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
    public void removeDataRoleMapping(String vdbName, String vdbVersion, String dataRole, String mappedRoleName) throws AdminException {
        VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
        synchronized(vdb) {
            DataPolicyMetadata policy = getPolicy(vdb, dataRole);
            policy.removeMappedRoleName(mappedRoleName);
        }
    }

    @Override
    public void setAnyAuthenticatedForDataRole(String vdbName, String vdbVersion, String dataRole, boolean anyAuthenticated) throws AdminException {
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
        embeddedServer.getConnectionFactoryProviders().remove(deployedName);
    }

    @Override
    public Collection<String> getDataSourceNames() throws AdminException {
        return new ArrayList<String>(embeddedServer.getConnectionFactoryProviders().keySet());
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
    public String getSchema(String vdbName, String vdbVersion, String modelName, EnumSet<SchemaObjectType> allowedTypes,
            String typeNamePattern) throws AdminException {
        if (vdbVersion == null) {
            vdbVersion = "1";
        }
        VDBMetaData vdb = checkVDB(vdbName, vdbVersion);
        MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
        if (modelName != null) {
            Schema schema = metadataStore.getSchema(modelName);
            return DDLStringVisitor.getDDLString(schema, allowedTypes, typeNamePattern);
        }
        Database db = DatabaseUtil.convert(vdb, metadataStore);
        return DDLStringVisitor.getDDLString(db);
    }

    public static String prettyFormat(String input) throws TransformerException {
        Source xmlInput = new StreamSource(new StringReader(input));
        StringWriter stringWriter = new StringWriter();
        StreamResult xmlOutput = new StreamResult(stringWriter);
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.transform(xmlInput, xmlOutput);
        return xmlOutput.getWriter().toString();
    }

    @Override
    public String getQueryPlan(String sessionId, long executionId)throws AdminException {
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

    @Override
    public List<String> getDeployments() throws AdminException {
        return Collections.emptyList();
    }
}