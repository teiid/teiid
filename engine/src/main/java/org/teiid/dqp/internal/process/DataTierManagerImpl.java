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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.RequestMessage;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorWork;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.BufferService;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.MatTableInfo;
import org.teiid.query.util.CommandContext;

/**
 * Full {@link ProcessorDataManager} implementation that 
 * controls access to {@link ConnectorManager}s and handles system queries.
 */
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
	
	private enum SystemAdminTables {
		MATVIEWS,
		VDBRESOURCES
	}
	
	private enum SystemProcs {
		GETXMLSCHEMAS
	}
	
	// Resources
	private DQPCore requestMgr;
    private BufferService bufferService;

    public DataTierManagerImpl(DQPCore requestMgr, BufferService bufferService) {
		this.requestMgr = requestMgr;
        this.bufferService = bufferService;
	}
    
	public TupleSource registerRequest(CommandContext context, Command command, String modelName, String connectorBindingId, int nodeID, int limit) throws TeiidComponentException, TeiidProcessingException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)context.getProcessorID());
		
		if(CoreConstants.SYSTEM_MODEL.equals(modelName) || CoreConstants.SYSTEM_ADMIN_MODEL.equals(modelName)) {
			return processSystemQuery(context, command, workItem.getDqpWorkContext());
		}
		
		AtomicRequestMessage aqr = createRequest(context.getProcessorID(), command, modelName, connectorBindingId, nodeID);
		if (limit > 0) {
			aqr.setFetchSize(Math.min(limit, aqr.getFetchSize()));
			throw new AssertionError();
		}
		ConnectorManagerRepository cmr = workItem.getDqpWorkContext().getVDB().getAttachment(ConnectorManagerRepository.class);
		ConnectorWork work = cmr.getConnectorManager(aqr.getConnectorName()).registerRequest(aqr);
        return new DataTierTupleSource(aqr, workItem, work, this, limit);
	}

	/**
	 * TODO: it would be good if processing here was lazy, in response of next batch, rather than up front.
	 * @param command
	 * @param workItem
	 * @return
	 * @throws TeiidComponentException
	 * @throws TeiidProcessingException 
	 */
	@SuppressWarnings("unchecked")
	private TupleSource processSystemQuery(CommandContext context, Command command,
			DQPWorkContext workContext) throws TeiidComponentException, TeiidProcessingException {
		String vdbName = workContext.getVdbName();
		int vdbVersion = workContext.getVdbVersion();
		VDBMetaData vdb = workContext.getVDB();
		CompositeMetadataStore metadata = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		TransformationMetadata indexMetadata = vdb.getAttachment(TransformationMetadata.class);
		Collection rows = new ArrayList();
		int oid = 1;
		if (command instanceof Query) {
			Query query = (Query)command;
			UnaryFromClause ufc = (UnaryFromClause)query.getFrom().getClauses().get(0);
			GroupSymbol group = ufc.getGroup();
			if (StringUtil.startsWithIgnoreCase(group.getNonCorrelationName(), (CoreConstants.SYSTEM_ADMIN_MODEL))) {
				final SystemAdminTables sysTable = SystemAdminTables.valueOf(group.getNonCorrelationName().substring(CoreConstants.SYSTEM_ADMIN_MODEL.length() + 1).toUpperCase());
				switch (sysTable) {
				case MATVIEWS:
					for (Schema schema : getVisibleSchemas(vdb, metadata)) {
						for (Table table : schema.getTables().values()) {
							if (!table.isMaterialized()) {
								continue;
							}
							String targetSchema = null;
							String matTableName = null;
							String state = null;
							Timestamp updated = null;
							Integer cardinaltity = null;
							Boolean valid = null;
							if (table.getMaterializedTable() == null) {
								TempTableStore globalStore = context.getGlobalTableStore();
								matTableName = RelationalPlanner.MAT_PREFIX+table.getFullName().toUpperCase();
								MatTableInfo info = globalStore.getMatTableInfo(matTableName);
								valid = info.isValid();
								state = info.getState().name();
								updated = info.getUpdateTime()==-1?null:new Timestamp(info.getUpdateTime());
								TempMetadataID id = globalStore.getMetadataStore().getTempGroupID(matTableName);
								if (id != null) {
									cardinaltity = id.getCardinality();
								}
								//ttl, pref_mem - not part of proper metadata
							} else {
								Table t = table.getMaterializedTable();
								matTableName = t.getName();
								targetSchema = t.getParent().getName();
							}
							rows.add(Arrays.asList(vdbName, schema.getName(), table.getName(), targetSchema, matTableName, valid, state, updated, cardinaltity));
						}
					}
					break;
				case VDBRESOURCES:
					String[] filePaths = indexMetadata.getVDBResourcePaths();
			        for (String filePath : filePaths) {
		        		rows.add(Arrays.asList(filePath, new BlobType(indexMetadata.getVDBResourceAsBlob(filePath))));
			        }
					break;
				}
				return new CollectionTupleSource(rows.iterator());
			}
			final SystemTables sysTable = SystemTables.valueOf(group.getNonCorrelationName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
			switch (sysTable) {
			case DATATYPES:
				for (Datatype datatype : metadata.getDatatypes()) {
					rows.add(Arrays.asList(datatype.getName(), datatype.isBuiltin(), datatype.isBuiltin(), datatype.getName(), datatype.getJavaClassName(), datatype.getScale(), 
							datatype.getLength(), datatype.getNullType().toString(), datatype.isSigned(), datatype.isAutoIncrement(), datatype.isCaseSensitive(), datatype.getPrecisionLength(), 
							datatype.getRadix(), datatype.getSearchType().toString(), datatype.getUUID(), datatype.getRuntimeTypeName(), datatype.getBasetypeName(), datatype.getAnnotation(), oid++));
				}
				break;
			case VIRTUALDATABASES:
				rows.add(Arrays.asList(vdbName, vdbVersion));
				break;
			case SCHEMAS:
				for (Schema model : getVisibleSchemas(vdb, metadata)) {
					rows.add(Arrays.asList(vdbName, model.getName(), model.isPhysical(), model.getUUID(), model.getAnnotation(), model.getPrimaryMetamodelUri(), oid++));
				}
				break;
			case PROCEDURES:
				for (Schema schema : getVisibleSchemas(vdb, metadata)) {
					for (Procedure proc : schema.getProcedures().values()) {
						rows.add(Arrays.asList(vdbName, proc.getParent().getName(), proc.getName(), proc.getNameInSource(), proc.getResultSet() != null, proc.getUUID(), proc.getAnnotation(), oid++));
					}
				}
				break;
			case PROCEDUREPARAMS:
				for (Schema schema : getVisibleSchemas(vdb, metadata)) {
					for (Procedure proc : schema.getProcedures().values()) {
						for (ProcedureParameter param : proc.getParameters()) {
							Datatype dt = param.getDatatype();
							rows.add(Arrays.asList(vdbName, proc.getParent().getName(), proc.getName(), param.getName(), dt!=null?dt.getRuntimeTypeName():null, param.getPosition(), param.getType().toString(), param.isOptional(), 
									param.getPrecision(), param.getLength(), param.getScale(), param.getRadix(), param.getNullType().toString(), param.getUUID(), param.getAnnotation(), oid++));
						}
						if (proc.getResultSet() != null) {
							for (Column param : proc.getResultSet().getColumns()) {
								Datatype dt = param.getDatatype();
								rows.add(Arrays.asList(vdbName, proc.getParent().getName(), proc.getName(), param.getName(), dt!=null?dt.getRuntimeTypeName():null, param.getPosition(), "ResultSet", false, //$NON-NLS-1$ 
										param.getPrecision(), param.getLength(), param.getScale(), param.getRadix(), param.getNullType().toString(), param.getUUID(), param.getAnnotation(), oid++));
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
						rows.add(Arrays.asList(entry.getKey(), entry.getValue(), record.getUUID(), oid++));
					}
				}
				break;
			default:
				for (Schema schema : getVisibleSchemas(vdb, metadata)) {
					for (Table table : schema.getTables().values()) {
						switch (sysTable) {
						case TABLES:
							rows.add(Arrays.asList(vdbName, schema.getName(), table.getName(), table.getTableType().toString(), table.getNameInSource(), 
									table.isPhysical(), table.supportsUpdate(), table.getUUID(), table.getCardinality(), table.getAnnotation(), table.isSystem(), table.isMaterialized(), oid++));
							break;
						case COLUMNS:
							for (Column column : table.getColumns()) {
								Datatype dt = column.getDatatype();
								rows.add(Arrays.asList(vdbName, schema.getName(), table.getName(), column.getName(), column.getPosition(), column.getNameInSource(), 
										dt!=null?dt.getRuntimeTypeName():null, column.getScale(), column.getLength(), column.isFixedLength(), column.isSelectable(), column.isUpdatable(),
										column.isCaseSensitive(), column.isSigned(), column.isCurrency(), column.isAutoIncremented(), column.getNullType().toString(), column.getMinimumValue(), 
										column.getMaximumValue(), column.getSearchType().toString(), column.getFormat(), column.getDefaultValue(), dt!=null?dt.getJavaClassName():null, column.getPrecision(), 
										column.getCharOctetLength(), column.getRadix(), column.getUUID(), column.getAnnotation(), oid++));
							}
							break;
						case KEYS:
							for (KeyRecord key : table.getAllKeys()) {
								rows.add(Arrays.asList(vdbName, table.getParent().getName(), table.getName(), key.getName(), key.getAnnotation(), key.getNameInSource(), key.getType().toString(), 
										false, (key instanceof ForeignKey)?((ForeignKey)key).getUniqueKeyID():null, key.getUUID(), oid++));
							}
							break;
						case KEYCOLUMNS:
							for (KeyRecord key : table.getAllKeys()) {
								int postition = 1;
								for (Column column : key.getColumns()) {
									rows.add(Arrays.asList(vdbName, schema.getName(), table.getName(), column.getName(), key.getName(), key.getType().toString(), 
											(key instanceof ForeignKey)?((ForeignKey)key).getUniqueKeyID():null, key.getUUID(), postition++, oid++));
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
			StoredProcedure proc = (StoredProcedure)command;			
			final SystemProcs sysTable = SystemProcs.valueOf(proc.getProcedureCallableName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
			switch (sysTable) {
			case GETXMLSCHEMAS:
				try {
					Object groupID = indexMetadata.getGroupID((String)((Constant)proc.getParameter(1).getExpression()).getValue());
					List<SQLXMLImpl> schemas = indexMetadata.getXMLSchemas(groupID);
					for (SQLXMLImpl schema : schemas) {
						rows.add(Arrays.asList(new XMLType(schema)));
					}
				} catch (QueryMetadataException e) {
					throw new TeiidProcessingException(e);
				}
				break;
			}
		}
		return new CollectionTupleSource(rows.iterator());
	}
	
	private List<Schema> getVisibleSchemas(VDBMetaData vdb, CompositeMetadataStore metadata) {
		ArrayList<Schema> result = new ArrayList<Schema>(); 
		for (Schema schema : metadata.getSchemas().values()) {
			if(vdb.isVisible(schema.getName())) {
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
        aqr.setMaxResultRows(requestMgr.getMaxSourceRows());
        aqr.setExceptionOnMaxRows(requestMgr.isExceptionOnMaxSourceRows());
        aqr.setPartialResults(request.supportsPartialResults());
        aqr.setSerial(requestMgr.getUserRequestSourceConcurrency() == 1);
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
	            throw new TeiidComponentException(QueryPlugin.Util.getString("DataTierManager.could_not_obtain_connector_binding", new Object[]{modelName, workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion() })); //$NON-NLS-1$
	        }
	        connectorBindingId = bindings.get(0); 
	        Assertion.isNotNull(connectorBindingId, "could not obtain connector id"); //$NON-NLS-1$
        }
        aqr.setConnectorName(connectorBindingId);
		return aqr;
	}
	
    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
    	throw new UnsupportedOperationException();
    }

    BufferManager getBufferManager() {
		return bufferService.getBufferManager();
	}
    
}
