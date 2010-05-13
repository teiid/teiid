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

package org.teiid.dqp.internal.process;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.RequestMessage;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.DQPPlugin;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.impl.ConnectorWork;
import org.teiid.dqp.internal.process.CodeTableCache.CacheKey;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.BufferService;
import org.teiid.language.SQLReservedWords;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.TransformationMetadata;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.TranslatorException;


public class DataTierManagerImpl implements ProcessorDataManager {
	
	private enum SystemTables {
		VIRTUALDATABASES,
		SCHEMAS,
		TABLES,
		DATATYPES,
		COLUMNS,
		KEYS,
		PROCEDURES,
		KEYCOLUMNS,
		PROCEDUREPARAMS,
		REFERENCEKEYCOLUMNS,
		PROPERTIES
	}
	
	private enum SystemProcs {
		GETCHARACTERVDBRESOURCE,
		GETBINARYVDBRESOURCE,
		GETVDBRESOURCEPATHS,
		GETXMLSCHEMAS
	}
	
	// Resources
	private DQPCore requestMgr;
    private BufferService bufferService;
    private ConnectorManagerRepository connectorManagerRepository;

	// Processor state
    private CodeTableCache codeTableCache;
    
    public DataTierManagerImpl(DQPCore requestMgr, ConnectorManagerRepository connectorRepo, BufferService bufferService, int maxCodeTables, int maxCodeRecords, int maxCodeTableRecords) {
		this.requestMgr = requestMgr;
        this.connectorManagerRepository = connectorRepo;
        this.bufferService = bufferService;
        this.codeTableCache = new CodeTableCache(maxCodeTables, maxCodeRecords, maxCodeTableRecords);
	}
    
    private ConnectorManager getCM(String connectorName) {
    	return this.connectorManagerRepository.getConnectorManager(connectorName);
    }

	public TupleSource registerRequest(Object processorId, Command command, String modelName, String connectorBindingId, int nodeID) throws TeiidComponentException, TeiidProcessingException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId);
		
		if(CoreConstants.SYSTEM_MODEL.equals(modelName)) {
			return processSystemQuery(command, workItem.getDqpWorkContext());
		}
		
		AtomicRequestMessage aqr = createRequest(processorId, command, modelName, connectorBindingId, nodeID);
        return new DataTierTupleSource(aqr.getCommand().getProjectedSymbols(), aqr, this, aqr.getConnectorName(), workItem);
	}

	/**
	 * TODO: it would be good if processing here was lazy, in response of next batch, rather than up front.
	 * @param command
	 * @param workItem
	 * @return
	 * @throws TeiidComponentException
	 */
	@SuppressWarnings("unchecked")
	private TupleSource processSystemQuery(Command command,
			DQPWorkContext workContext) throws TeiidComponentException {
		String vdbName = workContext.getVdbName();
		int vdbVersion = workContext.getVdbVersion();
		VDBMetaData vdb = workContext.getVDB();
		CompositeMetadataStore metadata = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		Collection rows = new ArrayList();
		if (command instanceof Query) {
			Query query = (Query)command;
			UnaryFromClause ufc = (UnaryFromClause)query.getFrom().getClauses().get(0);
			GroupSymbol group = ufc.getGroup();
			final SystemTables sysTable = SystemTables.valueOf(group.getNonCorrelationName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
			switch (sysTable) {
			case DATATYPES:
				rows = new LinkedHashSet(); //System types are duplicated in each indexed vdb... 
				for (Datatype datatype : metadata.getDatatypes()) {
					rows.add(Arrays.asList(datatype.getName(), datatype.isBuiltin(), datatype.isBuiltin(), datatype.getName(), datatype.getJavaClassName(), datatype.getScale(), 
							datatype.getLength(), datatype.getNullType().toString(), datatype.isSigned(), datatype.isAutoIncrement(), datatype.isCaseSensitive(), datatype.getPrecisionLength(), 
							datatype.getRadix(), datatype.getSearchType().toString(), datatype.getUUID(), datatype.getRuntimeTypeName(), datatype.getBasetypeName(), datatype.getAnnotation()));
				}
				break;
			case VIRTUALDATABASES:
				rows.add(Arrays.asList(vdbName, vdbVersion));
				break;
			case SCHEMAS:
				for (Schema model : getVisibleSchemas(vdb, metadata)) {
					rows.add(Arrays.asList(vdbName, model.getName(), model.isPhysical(), model.getUUID(), model.getAnnotation(), model.getPrimaryMetamodelUri()));
				}
				break;
			case PROCEDURES:
				for (Schema schema : getVisibleSchemas(vdb, metadata)) {
					for (Procedure proc : schema.getProcedures().values()) {
						rows.add(Arrays.asList(vdbName, proc.getParent().getName(), proc.getName(), proc.getNameInSource(), proc.getResultSet() != null, proc.getUUID(), proc.getAnnotation()));
					}
				}
				break;
			case PROCEDUREPARAMS:
				for (Schema schema : getVisibleSchemas(vdb, metadata)) {
					for (Procedure proc : schema.getProcedures().values()) {
						for (ProcedureParameter param : proc.getParameters()) {
							rows.add(Arrays.asList(vdbName, proc.getParent().getName(), proc.getName(), param.getName(), param.getDatatype().getRuntimeTypeName(), param.getPosition(), param.getType().toString(), param.isOptional(), 
									param.getPrecision(), param.getLength(), param.getScale(), param.getRadix(), param.getNullType().toString(), param.getUUID()));
						}
						if (proc.getResultSet() != null) {
							for (Column param : proc.getResultSet().getColumns()) {
								rows.add(Arrays.asList(vdbName, proc.getParent().getName(), proc.getName(), param.getName(), param.getDatatype().getRuntimeTypeName(), param.getPosition(), "ResultSet", false, //$NON-NLS-1$ 
										param.getPrecision(), param.getLength(), param.getScale(), param.getRadix(), param.getNullType().toString(), param.getUUID()));
							}
						}
					}
				}
				break;
			case PROPERTIES: //TODO: consider storing separately in the metadatastore 
				Collection<AbstractMetadataRecord> records = new LinkedHashSet<AbstractMetadataRecord>();
				records.addAll(metadata.getDatatypes());
				for (Schema schema : getVisibleSchemas(vdb, metadata)) {
					records.add(schema);
					records.addAll(schema.getTables().values());
					for (Table table : schema.getTables().values()) {
						records.add(table);
						records.addAll(table.getColumns());
						records.addAll(table.getAllKeys());
					}
					for (Procedure proc : schema.getProcedures().values()) {
						records.add(proc);
						records.addAll(proc.getParameters());
						if (proc.getResultSet() != null) {
							records.addAll(proc.getResultSet().getColumns());
						}
					}
				}
				for (AbstractMetadataRecord record : records) {
					for (Map.Entry<String, String> entry : record.getProperties().entrySet()) {
						rows.add(Arrays.asList(entry.getKey(), entry.getValue(), record.getUUID()));
					}
				}
				break;
			default:
				for (Schema schema : getVisibleSchemas(vdb, metadata)) {
					for (Table table : schema.getTables().values()) {
						switch (sysTable) {
						case TABLES:
							rows.add(Arrays.asList(vdbName, schema.getName(), table.getName(), table.getTableType().toString(), table.getNameInSource(), 
									table.isPhysical(), table.supportsUpdate(), table.getUUID(), table.getCardinality(), table.getAnnotation(), table.isSystem(), table.isMaterialized()));
							break;
						case COLUMNS:
							for (Column column : table.getColumns()) {
								if (column.getDatatype() == null) {
									continue; //some mapping classes don't set the datatype
								}
								rows.add(Arrays.asList(vdbName, schema.getName(), table.getName(), column.getName(), column.getPosition(), column.getNameInSource(), 
										column.getDatatype().getRuntimeTypeName(), column.getScale(), column.getLength(), column.isFixedLength(), column.isSelectable(), column.isUpdatable(),
										column.isCaseSensitive(), column.isSigned(), column.isCurrency(), column.isAutoIncremented(), column.getNullType().toString(), column.getMinimumValue(), 
										column.getMaximumValue(), column.getSearchType().toString(), column.getFormat(), column.getDefaultValue(), column.getDatatype().getJavaClassName(), column.getPrecision(), 
										column.getCharOctetLength(), column.getRadix(), column.getUUID(), column.getAnnotation()));
							}
							break;
						case KEYS:
							for (KeyRecord key : table.getAllKeys()) {
								rows.add(Arrays.asList(vdbName, table.getParent().getName(), table.getName(), key.getName(), key.getAnnotation(), key.getNameInSource(), key.getType().toString(), 
										false, (key instanceof ForeignKey)?((ForeignKey)key).getUniqueKeyID():null, key.getUUID()));
							}
							break;
						case KEYCOLUMNS:
							for (KeyRecord key : table.getAllKeys()) {
								int postition = 1;
								for (Column column : key.getColumns()) {
									rows.add(Arrays.asList(vdbName, schema.getName(), table.getName(), column.getName(), key.getName(), key.getType().toString(), 
											(key instanceof ForeignKey)?((ForeignKey)key).getUniqueKeyID():null, key.getUUID(), postition++));
								}
							}
							break;
						case REFERENCEKEYCOLUMNS:
							for (ForeignKey key : table.getForeignKeys()) {
								short postition = 0;
								for (Column column : key.getColumns()) {
									Table pkTable = key.getPrimaryKey().getParent();
									rows.add(Arrays.asList(vdbName, pkTable.getParent().getName(), pkTable.getName(), key.getPrimaryKey().getColumns().get(postition).getName(), vdbName, schema.getName(), table.getName(), column.getName(),
											++postition, DatabaseMetaData.importedKeyNoAction, DatabaseMetaData.importedKeyNoAction, key.getName(), key.getPrimaryKey().getName(), DatabaseMetaData.importedKeyInitiallyDeferred));
								}
							}
							break;
						}
					}
				}
				break;
			}
		} else {					
			TransformationMetadata indexMetadata = vdb.getAttachment(TransformationMetadata.class);
			StoredProcedure proc = (StoredProcedure)command;			
			final SystemProcs sysTable = SystemProcs.valueOf(proc.getProcedureCallableName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
			switch (sysTable) {
			case GETVDBRESOURCEPATHS:
		        String[] filePaths = indexMetadata.getVDBResourcePaths();
		        for (String filePath : filePaths) {
		        	rows.add(Arrays.asList(filePath, filePath.endsWith(".INDEX"))); //$NON-NLS-1$
		        }
				break;
			case GETBINARYVDBRESOURCE:
				String filePath = (String)((Constant)proc.getParameter(1).getExpression()).getValue();
				BlobImpl contents = indexMetadata.getVDBResourceAsBlob(filePath);
				if (contents != null) {
					rows.add(Arrays.asList(new BlobType(contents)));
				}
				break;
			case GETCHARACTERVDBRESOURCE:
				filePath = (String)((Constant)proc.getParameter(1).getExpression()).getValue();
				ClobImpl filecontents = indexMetadata.getVDBResourceAsClob(filePath);
				if (filecontents != null) {
					rows.add(Arrays.asList(new ClobType(filecontents)));
				}
				break;
			case GETXMLSCHEMAS:
				Object groupID = indexMetadata.getGroupID((String)((Constant)proc.getParameter(1).getExpression()).getValue());
				List<SQLXMLImpl> schemas = indexMetadata.getXMLSchemas(groupID);
				for (SQLXMLImpl schema : schemas) {
					rows.add(Arrays.asList(new XMLType(schema)));
				}
				break;
			}
		}
		return new CollectionTupleSource(rows.iterator(), command.getProjectedSymbols());
	}
	
	private List<Schema> getVisibleSchemas(VDBMetaData vdb, CompositeMetadataStore metadata) {
		ArrayList<Schema> result = new ArrayList<Schema>(); 
		for (Schema schema : metadata.getSchemas().values()) {
			ModelMetaData model = vdb.getModel(schema.getName());
			if(model.isVisible()) {
				result.add(schema);
			}
		}
		return result;
	}
	
	private AtomicRequestMessage createRequest(Object processorId,
			Command command, String modelName, String connectorBindingId, int nodeID)
			throws TeiidProcessingException, TeiidComponentException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId);
		
	    RequestMessage request = workItem.requestMsg;
		// build the atomic request based on original request + context info
        AtomicRequestMessage aqr = new AtomicRequestMessage(request, workItem.getDqpWorkContext(), nodeID);
        aqr.setCommand(command);
        aqr.setModelName(modelName);
        aqr.setPartialResults(request.supportsPartialResults());
        if (nodeID >= 0) {
        	aqr.setTransactionContext(workItem.getTransactionContext());
        }
        aqr.setFetchSize(this.bufferService.getBufferManager().getConnectorBatchSize());
        if (connectorBindingId == null) {
        	VDBMetaData vdb = workItem.getDqpWorkContext().getVDB();
        	ModelMetaData model = vdb.getModel(modelName);
        	List<String> bindings = model.getSourceNames();
	        if (bindings == null || bindings.size() != 1) {
	            // this should not happen, but it did occur when setting up the SystemAdmin models
	            throw new TeiidComponentException(DQPPlugin.Util.getString("DataTierManager.could_not_obtain_connector_binding", new Object[]{modelName, workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion() })); //$NON-NLS-1$
	        }
	        connectorBindingId = bindings.get(0); 
	        Assertion.isNotNull(connectorBindingId, "could not obtain connector id"); //$NON-NLS-1$
        }
        aqr.setConnectorName(connectorBindingId);
		return aqr;
	}
	
	ConnectorWork executeRequest(AtomicRequestMessage aqr, AbstractWorkItem awi, String connectorName) throws TranslatorException {
		return getCM(connectorName).executeRequest(aqr, awi);
	}

    /** 
     * Notify each waiting request that the code table data is now available.
     * @param requests
     * @since 4.2
     */
    private void notifyWaitingCodeTableRequests(Collection requests) {
        if (requests != null) {
            for (Iterator reqIter = requests.iterator(); reqIter.hasNext();) {
                RequestWorkItem workItem = requestMgr.safeGetWorkItem(reqIter.next());
                if (workItem != null) {
                	workItem.moreWork();
                }
            }
        }
    }        
    
    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        switch (this.codeTableCache.cacheExists(codeTableName, returnElementName, keyElementName, context)) {
        	case CACHE_NOT_EXIST:
	        	registerCodeTableRequest(context, codeTableName, returnElementName, keyElementName);
        	case CACHE_EXISTS:
	        	return this.codeTableCache.lookupValue(codeTableName, returnElementName, keyElementName, keyValue, context);
	        case CACHE_OVERLOAD:
	        	throw new TeiidProcessingException("ERR.018.005.0100", DQPPlugin.Util.getString("ERR.018.005.0100", "maxCodeTables")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	        default:
	            throw BlockedException.INSTANCE;
        }
    }

    void registerCodeTableRequest(
        final CommandContext context,
        final String codeTableName,
        String returnElementName,
        String keyElementName)
        throws TeiidComponentException, TeiidProcessingException {

        String query = SQLReservedWords.SELECT + ' ' + keyElementName + " ," + returnElementName + ' ' + SQLReservedWords.FROM + ' ' + codeTableName; //$NON-NLS-1$ 
        
        final CacheKey codeRequestId = this.codeTableCache.createCacheRequest(codeTableName, returnElementName, keyElementName, context);

        boolean success = false;
        try {
        	QueryProcessor processor = context.getQueryProcessorFactory().createQueryProcessor(query, codeTableName.toUpperCase(), context);
        	processor.setNonBlocking(true); //process lookup as fully blocking
            while (true) {
            	TupleBatch batch = processor.nextBatch();
            	codeTableCache.loadTable(codeRequestId, batch.getAllTuples());	
            	if (batch.getTerminationFlag()) {
            		break;
            	}
            }
        	success = true;
        } finally {
        	Collection requests = codeTableCache.markCacheDone(codeRequestId, success);
        	notifyWaitingCodeTableRequests(requests);
        }
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.dqp.internal.process.DataTierManager#clearCodeTables()
	 */
    public void clearCodeTables() {
        this.codeTableCache.clearAll();
    }
    
}
