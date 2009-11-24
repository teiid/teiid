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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.Datatype;
import org.teiid.connector.metadata.runtime.ForeignKey;
import org.teiid.connector.metadata.runtime.KeyRecord;
import org.teiid.connector.metadata.runtime.ProcedureParameter;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.Schema;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.dqp.internal.process.CodeTableCache.CacheKey;
import org.teiid.metadata.CompositeMetadataStore;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.metadata.runtime.api.MetadataSourceUtil;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.CommandContext;

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
		GETVDBRESOURCEPATHS
	}
	
	private class CollectionTupleSource implements TupleSource {
		
		private Iterator<List<Object>> tuples;
		private List<SingleElementSymbol> schema;
		
		public CollectionTupleSource(Iterator<List<Object>> tuples,
				List<SingleElementSymbol> schema) {
			this.tuples = tuples;
			this.schema = schema;
		}

		@Override
		public List<?> nextTuple() throws MetaMatrixComponentException,
				MetaMatrixProcessingException {
			if (tuples.hasNext()) {
				return tuples.next();
			}
			return null;
		}
		
		@Override
		public List<SingleElementSymbol> getSchema() {
			return schema;
		}
		
		@Override
		public void closeSource() throws MetaMatrixComponentException {
			
		}
	}

	// Resources
	private DQPCore requestMgr;
    private DataService dataService;
    private VDBService vdbService;
    private BufferService bufferService;
    private MetadataService metadataService;

	// Processor state
    private CodeTableCache codeTableCache;
    
    public DataTierManagerImpl(DQPCore requestMgr,
        DataService dataService, VDBService vdbService, BufferService bufferService, MetadataService metadataService, 
        int maxCodeTables, int maxCodeRecords, int maxCodeTableRecords) {

		this.requestMgr = requestMgr;
        this.dataService = dataService;
        this.vdbService = vdbService;
        this.bufferService = bufferService;
        this.metadataService = metadataService;

        this.codeTableCache = new CodeTableCache(maxCodeTables, maxCodeRecords, maxCodeTableRecords);
	}

	public TupleSource registerRequest(Object processorId, Command command,
			String modelName, String connectorBindingId, int nodeID) throws MetaMatrixComponentException, MetaMatrixProcessingException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId);
		
		if(CoreConstants.SYSTEM_MODEL.equals(modelName)) {
			return processSystemQuery(command, workItem);
		}
		
		AtomicRequestMessage aqr = createRequest(processorId, command, modelName, connectorBindingId, nodeID);
        DataTierTupleSource tupleSource = new DataTierTupleSource(aqr.getCommand().getProjectedSymbols(), aqr, this, aqr.getConnectorID(), workItem);
        tupleSource.open();
        return tupleSource;
	}

	/**
	 * TODO: it would be good if processing here was lazy, in response of next batch, rather than up front.
	 * @param command
	 * @param workItem
	 * @return
	 * @throws MetaMatrixComponentException
	 */
	@SuppressWarnings("unchecked")
	private TupleSource processSystemQuery(Command command,
			RequestWorkItem workItem) throws MetaMatrixComponentException {
		String vdbName = workItem.getDqpWorkContext().getVdbName();
		String vdbVersion = workItem.getDqpWorkContext().getVdbVersion();
		CompositeMetadataStore metadata = this.metadataService.getMetadataObjectSource(vdbName, vdbVersion);
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
				for (Schema model : getVisibleSchemas(vdbName, vdbVersion, metadata)) {
					rows.add(Arrays.asList(vdbName, model.getName(), model.isPhysical(), model.getUUID(), model.getAnnotation(), model.getPrimaryMetamodelUri()));
				}
				break;
			case PROCEDURES:
				for (Schema schema : getVisibleSchemas(vdbName, vdbVersion, metadata)) {
					for (ProcedureRecordImpl proc : schema.getProcedures().values()) {
						rows.add(Arrays.asList(vdbName, proc.getSchema().getName(), proc.getName(), proc.getNameInSource(), proc.getResultSet() != null, proc.getUUID(), proc.getAnnotation()));
					}
				}
				break;
			case PROCEDUREPARAMS:
				for (Schema schema : getVisibleSchemas(vdbName, vdbVersion, metadata)) {
					for (ProcedureRecordImpl proc : schema.getProcedures().values()) {
						for (ProcedureParameter param : proc.getParameters()) {
							rows.add(Arrays.asList(vdbName, proc.getSchema().getName(), proc.getName(), param.getName(), param.getDatatype().getRuntimeTypeName(), param.getPosition(), param.getType().toString(), param.isOptional(), 
									param.getPrecision(), param.getLength(), param.getScale(), param.getRadix(), param.getNullType().toString(), param.getUUID()));
						}
						if (proc.getResultSet() != null) {
							for (Column param : proc.getResultSet().getColumns()) {
								rows.add(Arrays.asList(vdbName, proc.getSchema().getName(), proc.getName(), param.getName(), param.getDatatype().getRuntimeTypeName(), param.getPosition(), ProcedureParameter.Type.ResultSet.toString(), false, 
										param.getPrecision(), param.getLength(), param.getScale(), param.getRadix(), param.getNullType().toString(), param.getUUID()));
							}
						}
					}
				}
				break;
			case PROPERTIES: //TODO: consider storing separately in the metadatastore 
				Collection<AbstractMetadataRecord> records = new LinkedHashSet<AbstractMetadataRecord>();
				records.addAll(metadata.getDatatypes());
				for (Schema schema : getVisibleSchemas(vdbName, vdbVersion, metadata)) {
					records.add(schema);
					records.addAll(schema.getTables().values());
					for (Table table : schema.getTables().values()) {
						records.add(table);
						records.addAll(table.getColumns());
						records.addAll(table.getAllKeys());
					}
					for (ProcedureRecordImpl proc : schema.getProcedures().values()) {
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
				for (Schema schema : getVisibleSchemas(vdbName, vdbVersion, metadata)) {
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
										column.isCaseSensitive(), column.isSigned(), column.isCurrency(), column.isAutoIncrementable(), column.getNullType().toString(), column.getMinValue(), 
										column.getMaxValue(), column.getSearchType().toString(), column.getFormat(), column.getDefaultValue(), column.getDatatype().getJavaClassName(), column.getPrecision(), 
										column.getCharOctetLength(), column.getRadix(), column.getUUID(), column.getAnnotation()));
							}
							break;
						case KEYS:
							for (KeyRecord key : table.getAllKeys()) {
								rows.add(Arrays.asList(vdbName, table.getSchema().getName(), table.getName(), key.getName(), key.getAnnotation(), key.getNameInSource(), key.getType().toString(), 
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
								int postition = 0;
								for (Column column : key.getColumns()) {
									Table pkTable = key.getPrimaryKey().getTable();
									rows.add(Arrays.asList(vdbName, pkTable.getSchema().getName(), pkTable.getName(), key.getPrimaryKey().getColumns().get(postition).getName(), vdbName, schema.getName(), table.getName(), column.getName(),
											++postition, (short)DatabaseMetaData.importedKeyNoAction, (short)DatabaseMetaData.importedKeyNoAction, key.getName(), key.getPrimaryKey().getName(), (short)DatabaseMetaData.importedKeyInitiallyDeferred));
								}
							}
							break;
						}
					}
				}
				break;
			}
		} else {					
			StoredProcedure proc = (StoredProcedure)command;
			final SystemProcs sysTable = SystemProcs.valueOf(proc.getProcedureCallableName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
			switch (sysTable) {
			case GETVDBRESOURCEPATHS:
		        Set<String> filePaths = metadata.getMetadataSource().getEntries();
		        for (String filePath : filePaths) {
		        	if (vdbService.getFileVisibility(vdbName, vdbVersion, filePath) != ModelInfo.PUBLIC) {
		        		continue;
		        	}
		        	rows.add(Arrays.asList(filePath, filePath.endsWith(".INDEX"))); //$NON-NLS-1$
		        }
				break;
			case GETBINARYVDBRESOURCE:
				String filePath = (String)proc.getParameter(0).getValue();
				if (metadata.getMetadataSource().getEntries().contains(filePath) && vdbService.getFileVisibility(vdbName, vdbVersion, filePath) == ModelInfo.PUBLIC) {
					try {
						rows.add(Arrays.asList(new SerialBlob(MetadataSourceUtil.getFileContentAsString(filePath, metadata.getMetadataSource()).getBytes())));
					} catch (SQLException e) {
						throw new MetaMatrixComponentException(e);
					}
				}
				break;
			case GETCHARACTERVDBRESOURCE:
				filePath = (String)proc.getParameter(0).getValue();
				if (metadata.getMetadataSource().getEntries().contains(filePath) && vdbService.getFileVisibility(vdbName, vdbVersion, filePath) == ModelInfo.PUBLIC) {
					try {
						rows.add(Arrays.asList(new SerialClob(MetadataSourceUtil.getFileContentAsString(filePath, metadata.getMetadataSource()).toCharArray())));
					} catch (SQLException e) {
						throw new MetaMatrixComponentException(e);
					}
				}
				break;
			}
		}
		return new CollectionTupleSource(rows.iterator(), command.getProjectedSymbols());
	}
	
	private List<Schema> getVisibleSchemas(String vdbName, String vdbVersion, CompositeMetadataStore metadata) throws MetaMatrixComponentException {
		ArrayList<Schema> result = new ArrayList<Schema>(); 
		for (Schema schema : metadata.getSchemas().values()) {
			if(vdbService.getModelVisibility(vdbName, vdbVersion, schema.getName()) == ModelInfo.PUBLIC) {
				result.add(schema);
			}
		}
		return result;
	}
	
	private AtomicRequestMessage createRequest(Object processorId,
			Command command, String modelName, String connectorBindingId, int nodeID)
			throws MetaMatrixProcessingException, MetaMatrixComponentException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId);
		
	    RequestMessage request = workItem.requestMsg;
		// build the atomic request based on original request + context info
        AtomicRequestMessage aqr = new AtomicRequestMessage(request, workItem.getDqpWorkContext(), nodeID);
        aqr.markSubmissionStart();
        aqr.setCommand(command);
        aqr.setModelName(modelName);
        aqr.setUseResultSetCache(request.useResultSetCache());
        aqr.setPartialResults(request.supportsPartialResults());
        if (nodeID >= 0) {
        	aqr.setTransactionContext(workItem.getTransactionContext());
        }
        aqr.setFetchSize(this.bufferService.getBufferManager().getConnectorBatchSize());
        if (connectorBindingId == null) {
        	List bindings = vdbService.getConnectorBindingNames(workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion(), modelName);
	        if (bindings == null || bindings.size() != 1) {
	            // this should not happen, but it did occur when setting up the SystemAdmin models
	            throw new MetaMatrixComponentException(DQPPlugin.Util.getString("DataTierManager.could_not_obtain_connector_binding", new Object[]{modelName, workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion() })); //$NON-NLS-1$
	        }
	        connectorBindingId = (String)bindings.get(0); 
	        Assertion.isNotNull(connectorBindingId, "could not obtain connector id"); //$NON-NLS-1$
        }
        aqr.setConnectorBindingID(connectorBindingId);
        // Select any connector instance for this connector binding
        ConnectorID connectorID = this.dataService.selectConnector(connectorBindingId);
        // if we had this as null before
        aqr.setConnectorID(connectorID);
		return aqr;
	}
	
	String getConnectorName(String connectorBindingID) {
        try {
            return vdbService.getConnectorName(connectorBindingID);
        } catch (MetaMatrixComponentException t) {
            // OK
        }
        return connectorBindingID;
	}
	
	void executeRequest(AtomicRequestMessage aqr, ConnectorID connectorId,
			ResultsReceiver<AtomicResultsMessage> receiver)
			throws MetaMatrixComponentException {
		this.dataService.executeRequest(aqr, connectorId, receiver);
	}

	public void closeRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		this.dataService.closeRequest(request, connectorId);
	}
	
	public void cancelRequest(AtomicRequestID request, ConnectorID connectorId)
		throws MetaMatrixComponentException {
		this.dataService.cancelRequest(request, connectorId);
	}

	void requestBatch(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		this.dataService.requestBatch(request, connectorId);
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
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        switch (this.codeTableCache.cacheExists(codeTableName, returnElementName, keyElementName, context)) {
        	case CACHE_NOT_EXIST:
	        	registerCodeTableRequest(context, codeTableName, returnElementName, keyElementName);
        	case CACHE_EXISTS:
	        	return this.codeTableCache.lookupValue(codeTableName, returnElementName, keyElementName, keyValue, context);
	        case CACHE_OVERLOAD:
	        	throw new MetaMatrixProcessingException("ERR.018.005.0100", DQPPlugin.Util.getString("ERR.018.005.0100", DQPEmbeddedProperties.MAX_CODE_TABLES)); //$NON-NLS-1$ //$NON-NLS-2$
	        default:
	            throw BlockedException.INSTANCE;
        }
    }

    void registerCodeTableRequest(
        final CommandContext context,
        final String codeTableName,
        String returnElementName,
        String keyElementName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {

        String query = ReservedWords.SELECT + ' ' + keyElementName + " ," + returnElementName + ' ' + ReservedWords.FROM + ' ' + codeTableName; //$NON-NLS-1$ 
        
        final CacheKey codeRequestId = this.codeTableCache.createCacheRequest(codeTableName, returnElementName, keyElementName, context);

        boolean success = false;
        QueryProcessor processor = null;
        try {
            processor = context.getQueryProcessorFactory().createQueryProcessor(query, codeTableName.toUpperCase(), context);

            processor.setBatchHandler(new QueryProcessor.BatchHandler() {
            	@Override
            	public void batchProduced(TupleBatch batch) throws MetaMatrixProcessingException {
               		codeTableCache.loadTable(codeRequestId, batch.getAllTuples());
            	}
            });

        	//process lookup as fully blocking
        	processor.process();
        	success = true;
        } finally {
        	Collection requests = null;
        	if (success) {
                requests = codeTableCache.markCacheLoaded(codeRequestId);
        	} else {
        		requests = codeTableCache.errorLoadingCache(codeRequestId);        		
        	}
        	notifyWaitingCodeTableRequests(requests);
        	if (processor != null) {
	            try {
	            	this.bufferService.getBufferManager().removeTupleSource(processor.getResultsID());
	    		} catch (MetaMatrixComponentException e1) {
	    			LogManager.logDetail(LogConstants.CTX_DQP, "Exception closing code table request"); //$NON-NLS-1$
	    		}
        	}
        }
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.dqp.internal.process.DataTierManager#clearCodeTables()
	 */
    public void clearCodeTables() {
        this.codeTableCache.clearAll();
    }

}
