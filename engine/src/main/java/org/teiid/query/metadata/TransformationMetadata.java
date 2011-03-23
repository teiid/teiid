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

package org.teiid.query.metadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingLoader;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.sql.lang.SPParameter;


/**
 * Teiid's implementation of the QueryMetadataInterface that reads columns, groups, models etc.
 * from the metadata object model.
 */
public class TransformationMetadata extends BasicQueryMetadata implements Serializable {
	
	private final class VirtualFileInputStreamFactory extends
			InputStreamFactory {
		private final VirtualFile f;

		private VirtualFileInputStreamFactory(VirtualFile f) {
			this.f = f;
		}

		@Override
		public InputStream getInputStream() throws IOException {
			return f.openStream();
		}
		
		@Override
		public long getLength() {
			try {
				return f.getSize();
			} catch (IOException e) {
			}
			return super.getLength();
		}
		
		@Override
		public void free() throws IOException {
			f.close();
		}
	}

	public static class Resource {
		public Resource(VirtualFile file, boolean visible) {
			this.file = file;
			this.visible = visible;
		}
		VirtualFile file;
		boolean visible;
	}
	
	private static final long serialVersionUID = 1058627332954475287L;
	
	/** Delimiter character used when specifying fully qualified entity names */
    public static final char DELIMITER_CHAR = StringUtil.Constants.DOT_CHAR;
    public static final String DELIMITER_STRING = String.valueOf(DELIMITER_CHAR);
    
    // error message cached to avoid i18n lookup each time
    public static String NOT_EXISTS_MESSAGE = StringUtil.Constants.SPACE+QueryPlugin.Util.getString("TransformationMetadata.does_not_exist._1"); //$NON-NLS-1$

    private static Properties EMPTY_PROPS = new Properties();
    
    private final CompositeMetadataStore store;
    private Map<String, Resource> vdbEntries;
    private FunctionLibrary functionLibrary;
    private VDBMetaData vdbMetaData;
    
    /*
     * TODO: move caching to jboss cache structure
     */
    private final Map<String, Object> metadataCache = Collections.synchronizedMap(new LRUCache<String, Object>(250));
    private final Map<String, Object> groupInfoCache = Collections.synchronizedMap(new LRUCache<String, Object>(250));
    private final Map<String, Collection<Table>> partialNameToFullNameCache = Collections.synchronizedMap(new LRUCache<String, Collection<Table>>(1000));
    private final Map<String, Collection<StoredProcedureInfo>> procedureCache = Collections.synchronizedMap(new LRUCache<String, Collection<StoredProcedureInfo>>(200));
    /**
     * TransformationMetadata constructor
     * @param context Object containing the info needed to lookup metadta.
     */
    public TransformationMetadata(VDBMetaData vdbMetadata, final CompositeMetadataStore store, Map<String, Resource> vdbEntries, FunctionTree systemFunctions, Collection<FunctionTree> functionTrees) {
    	ArgCheck.isNotNull(store);
    	this.vdbMetaData = vdbMetadata;
        this.store = store;
        if (vdbEntries == null) {
        	this.vdbEntries = Collections.emptyMap();
        } else {
        	this.vdbEntries = vdbEntries;
        }
        if (functionTrees == null) {
        	this.functionLibrary = new FunctionLibrary(systemFunctions);
        } else {
            this.functionLibrary = new FunctionLibrary(systemFunctions, functionTrees.toArray(new FunctionTree[functionTrees.size()]));
        }
    }
    
    private TransformationMetadata(final CompositeMetadataStore store, FunctionLibrary functionLibrary) {
    	ArgCheck.isNotNull(store);
        this.store = store;
    	this.vdbEntries = Collections.emptyMap();
        this.functionLibrary = functionLibrary;
    }
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    public Object getElementID(final String elementName) throws TeiidComponentException, QueryMetadataException {
    	int columnIndex = elementName.lastIndexOf(TransformationMetadata.DELIMITER_STRING);
		if (columnIndex == -1) {
			throw new QueryMetadataException(elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		Table table = this.store.findGroup(elementName.substring(0, columnIndex).toUpperCase());
		String shortElementName = elementName.substring(columnIndex + 1);
		for (Column column : (List<Column>)getElementIDsInGroupID(table)) {
			if (column.getName().equalsIgnoreCase(shortElementName)) {
				return column;
			}
        }
        throw new QueryMetadataException(elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
    }

    public Table getGroupID(final String groupName) throws TeiidComponentException, QueryMetadataException {
        return getMetadataStore().findGroup(groupName.toUpperCase());
    }
    
    public Collection<String> getGroupsForPartialName(final String partialGroupName)
        throws TeiidComponentException, QueryMetadataException {
		ArgCheck.isNotEmpty(partialGroupName);

		Collection<Table> matches = this.partialNameToFullNameCache.get(partialGroupName);
		
		if (matches == null) {
			String partialName = DELIMITER_CHAR + partialGroupName.toUpperCase(); 
	
	        matches = getMetadataStore().getGroupsForPartialName(partialName);
	        
        	this.partialNameToFullNameCache.put(partialGroupName, matches);
		}
		
		if (matches.isEmpty()) {
			return Collections.emptyList();
		}
		
		Collection<String> filteredResult = new ArrayList<String>(matches.size());
		for (Table table : matches) {
			if (vdbMetaData == null || vdbMetaData.isVisible(table.getParent().getName())) {
	        	filteredResult.add(table.getFullName());
	        }
		}
		return filteredResult;
    }

    public Object getModelID(final Object groupOrElementID) throws TeiidComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(AbstractMetadataRecord.class, groupOrElementID);
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) groupOrElementID;
        AbstractMetadataRecord parent = metadataRecord.getParent();
        if (parent instanceof Schema) {
        	return parent;
        }
        if (parent == null) {
        	throw createInvalidRecordTypeException(groupOrElementID);
        }
        parent = parent.getParent();
        if (parent instanceof Schema) {
        	return parent;
        }
    	throw createInvalidRecordTypeException(groupOrElementID);
    }

    public String getFullName(final Object metadataID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) metadataID;
        return metadataRecord.getFullName();
    }
    
    @Override
    public String getName(Object metadataID) throws TeiidComponentException,
    		QueryMetadataException {
    	ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) metadataID;
        return metadataRecord.getName();
    }

    public List getElementIDsInGroupID(final Object groupID) throws TeiidComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	return ((Table)groupID).getColumns();
    }

    public Object getGroupIDForElementID(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            Column columnRecord = (Column) elementID;
            AbstractMetadataRecord parent = columnRecord.getParent();
            if (parent instanceof Table) {
            	return parent;
            }
        } 
        throw createInvalidRecordTypeException(elementID);
    }
    
    public boolean hasProcedure(String name) throws TeiidComponentException {
    	try {
    		return getStoredProcedureInfoForProcedure(name) != null;
    	} catch (QueryMetadataException e) {
    		return false;
    	}
    }

    public StoredProcedureInfo getStoredProcedureInfoForProcedure(final String name)
        throws TeiidComponentException, QueryMetadataException {
        StoredProcedureInfo result = getStoredProcInfoDirect(name);
        
		if (result == null) {
			throw new QueryMetadataException(name+NOT_EXISTS_MESSAGE);
		}
    	
        return result;
    }

	private StoredProcedureInfo getStoredProcInfoDirect(
			final String name)
			throws TeiidComponentException, QueryMetadataException {
		ArgCheck.isNotEmpty(name);
        String canonicalName = name.toUpperCase();
        Collection<StoredProcedureInfo> results = this.procedureCache.get(canonicalName);
        
        if (results == null) {
        	Collection<Procedure> procRecords = getMetadataStore().getStoredProcedure(canonicalName); 
        	results = new ArrayList<StoredProcedureInfo>(procRecords.size());
        	for (Procedure procRecord : procRecords) {
                String procedureFullName = procRecord.getFullName();

                // create the storedProcedure info object that would hold procedure's metadata
                StoredProcedureInfo procInfo = new StoredProcedureInfo();
                procInfo.setProcedureCallableName(procedureFullName);
                procInfo.setProcedureID(procRecord);

                // modelID for the procedure
                procInfo.setModelID(procRecord.getParent());

                // get the parameter metadata info
                for (ProcedureParameter paramRecord : procRecord.getParameters()) {
                    String runtimeType = paramRecord.getRuntimeType();
                    int direction = this.convertParamRecordTypeToStoredProcedureType(paramRecord.getType());
                    // create a parameter and add it to the procedure object
                    SPParameter spParam = new SPParameter(paramRecord.getPosition(), direction, paramRecord.getFullName());
                    spParam.setMetadataID(paramRecord);
                    spParam.setClassType(DataTypeManager.getDataTypeClass(runtimeType));
                    procInfo.addParameter(spParam);
                }

                // if the procedure returns a resultSet, obtain resultSet metadata
                if(procRecord.getResultSet() != null) {
                    ColumnSet<Procedure> resultRecord = procRecord.getResultSet();
                    // resultSet is the last parameter in the procedure
                    int lastParamIndex = procInfo.getParameters().size() + 1;
                    SPParameter param = new SPParameter(lastParamIndex, SPParameter.RESULT_SET, resultRecord.getFullName());
                    param.setClassType(java.sql.ResultSet.class);           
                    param.setMetadataID(resultRecord);

                    for (Column columnRecord : resultRecord.getColumns()) {
                        String colType = columnRecord.getRuntimeType();
                        param.addResultSetColumn(columnRecord.getFullName(), DataTypeManager.getDataTypeClass(colType), columnRecord);
                    }

                    procInfo.addParameter(param);            
                }

                // if this is a virtual procedure get the procedure plan
                if(procRecord.isVirtual()) {
                    QueryNode queryNode = new QueryNode(procedureFullName, procRecord.getQueryPlan()); 
                    procInfo.setQueryPlan(queryNode);
                }
                
                //subtract 1, to match up with the server
                procInfo.setUpdateCount(procRecord.getUpdateCount() -1);
				results.add(procInfo);
			}
        	this.procedureCache.put(canonicalName, results);        	
        }
        
        StoredProcedureInfo result = null;
        
        for (StoredProcedureInfo storedProcedureInfo : results) {
        	Schema schema = (Schema)storedProcedureInfo.getModelID();
	        if(name.equalsIgnoreCase(storedProcedureInfo.getProcedureCallableName()) || vdbMetaData == null || vdbMetaData.isVisible(schema.getName())){
	        	if (result != null) {
	    			throw new QueryMetadataException(QueryPlugin.Util.getString("ambiguous_procedure", name)); //$NON-NLS-1$
	    		}
	        	result = storedProcedureInfo;
	        }
		}
		return result;
	}
    
    /**
     * Method to convert the parameter type returned from a ProcedureParameterRecord
     * to the parameter type expected by StoredProcedureInfo
     * @param parameterType
     * @return
     */
    private int convertParamRecordTypeToStoredProcedureType(final ProcedureParameter.Type parameterType) {
        switch (parameterType) {
            case In : return SPParameter.IN;
            case Out : return SPParameter.OUT;
            case InOut : return SPParameter.INOUT;
            case ReturnValue : return SPParameter.RETURN_VALUE;
            default : 
                return -1;
        }
    }

    public String getElementType(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getRuntimeType();            
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getRuntimeType();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getDefaultValue(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getDefaultValue();            
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getDefaultValue();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMinimumValue(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getMinimumValue();            
        } else if(elementID instanceof ProcedureParameter){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMaximumValue(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getMaximumValue();            
        } else if(elementID instanceof ProcedureParameter){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public boolean isVirtualGroup(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        return ((Table) groupID).isVirtual();
    }

    /** 
     * @see org.teiid.query.metadata.QueryMetadataInterface#isProcedureInputElement(java.lang.Object)
     * @since 4.2
     */
    public boolean isProcedure(final Object groupID) throws TeiidComponentException, QueryMetadataException {
    	if(groupID instanceof Procedure) {
            return true;            
        } 
    	if(groupID instanceof Table){
            return false;
        } 
    	throw createInvalidRecordTypeException(groupID);
    }

    public boolean isVirtualModel(final Object modelID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Schema.class, modelID);
        Schema modelRecord = (Schema) modelID;
        return !modelRecord.isPhysical();
    }

    public QueryNode getVirtualPlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);

        Table tableRecord = (Table) groupID;
        if (!tableRecord.isVirtual()) {
            throw new QueryMetadataException(QueryPlugin.Util.getString("TransformationMetadata.QueryPlan_could_not_be_found_for_physical_group__6")+tableRecord.getFullName()); //$NON-NLS-1$
        }
        String transQuery = tableRecord.getSelectTransformation();
        QueryNode queryNode = new QueryNode(tableRecord.getFullName(), transQuery);

        // get any bindings and add them onto the query node
        List bindings = tableRecord.getBindings();
        if(bindings != null) {
            for(Iterator bindIter = bindings.iterator();bindIter.hasNext();) {
                queryNode.addBinding((String)bindIter.next());
            }
        }

        return queryNode;
    }

    public String getInsertPlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(QueryPlugin.Util.getString("TransformationMetadata.InsertPlan_could_not_be_found_for_physical_group__8")+tableRecordImpl.getFullName()); //$NON-NLS-1$
        }
        return ((Table)groupID).getInsertPlan();
    }

    public String getUpdatePlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
        	throw new QueryMetadataException(QueryPlugin.Util.getString("TransformationMetadata.InsertPlan_could_not_be_found_for_physical_group__10")+tableRecordImpl.getFullName());         //$NON-NLS-1$
        }
        return ((Table)groupID).getUpdatePlan();
    }

    public String getDeletePlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(QueryPlugin.Util.getString("TransformationMetadata.DeletePlan_could_not_be_found_for_physical_group__12")+tableRecordImpl.getFullName()); //$NON-NLS-1$
        }
        return ((Table)groupID).getDeletePlan();
    }

    public boolean modelSupports(final Object modelID, final int modelConstant)
        throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Schema.class, modelID);

        switch(modelConstant) {
            default:
                throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + modelConstant); //$NON-NLS-1$
        }        
    }

    public boolean groupSupports(final Object groupID, final int groupConstant)
        throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;

        switch(groupConstant) {
            case SupportConstants.Group.UPDATE:
                return tableRecord.supportsUpdate();
            default:
                throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + groupConstant); //$NON-NLS-1$
        }
    }

    public boolean elementSupports(final Object elementID, final int elementConstant)
        throws TeiidComponentException, QueryMetadataException {
        
        if(elementID instanceof Column) {
            Column columnRecord = (Column) elementID;            
            switch(elementConstant) {
                case SupportConstants.Element.NULL:
                    return columnRecord.getNullType() == NullType.Nullable;
                case SupportConstants.Element.NULL_UNKNOWN:
                    return columnRecord.getNullType() == NullType.Unknown;
                case SupportConstants.Element.SEARCHABLE_COMPARE:
                    return (columnRecord.getSearchType() == SearchType.Searchable || columnRecord.getSearchType() == SearchType.All_Except_Like);
                case SupportConstants.Element.SEARCHABLE_LIKE:
                	return (columnRecord.getSearchType() == SearchType.Searchable || columnRecord.getSearchType() == SearchType.Like_Only);
                case SupportConstants.Element.SELECT:
                    return columnRecord.isSelectable();
                case SupportConstants.Element.UPDATE:
                    return columnRecord.isUpdatable();
                case SupportConstants.Element.DEFAULT_VALUE:
                    Object defaultValue = columnRecord.getDefaultValue();
                    if(defaultValue == null) {
                        return false;
                    }
                    return true;
                case SupportConstants.Element.AUTO_INCREMENT:
                    return columnRecord.isAutoIncremented();
                case SupportConstants.Element.CASE_SENSITIVE:
                    return columnRecord.isCaseSensitive();
                case SupportConstants.Element.SIGNED:
                    return columnRecord.isSigned();
                default:
                    throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
            }
        } else if(elementID instanceof ProcedureParameter) {
            ProcedureParameter columnRecord = (ProcedureParameter) elementID;            
            switch(elementConstant) {
                case SupportConstants.Element.NULL:
                	return columnRecord.getNullType() == NullType.Nullable;
                case SupportConstants.Element.NULL_UNKNOWN:
                	return columnRecord.getNullType() == NullType.Unknown;
                case SupportConstants.Element.SEARCHABLE_COMPARE:
                case SupportConstants.Element.SEARCHABLE_LIKE:
                    return false;
                case SupportConstants.Element.SELECT:
                    return columnRecord.getType() != Type.In;
                case SupportConstants.Element.UPDATE:
                    return false;
                case SupportConstants.Element.DEFAULT_VALUE:
                    Object defaultValue = columnRecord.getDefaultValue();
                    if(defaultValue == null) {
                        return false;
                    }
                    return true;
                case SupportConstants.Element.AUTO_INCREMENT:
                    return false;
                case SupportConstants.Element.CASE_SENSITIVE:
                    return false;
                case SupportConstants.Element.SIGNED:
                    return true;
                default:
                    throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
            }
            
        } else {            
            throw createInvalidRecordTypeException(elementID);
        }
    }
    
    private IllegalArgumentException createInvalidRecordTypeException(Object elementID) {
        return new IllegalArgumentException(QueryPlugin.Util.getString("TransformationMetadata.Invalid_type", elementID.getClass().getName()));         //$NON-NLS-1$
    }

    public int getMaxSetSize(final Object modelID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Schema.class, modelID);
        return 0;
    }

    public Collection getIndexesInGroup(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        return ((Table)groupID).getIndexes();
    }

    public Collection getUniqueKeysInGroup(final Object groupID)
        throws TeiidComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	Table tableRecordImpl = (Table)groupID;
    	ArrayList<ColumnSet> result = new ArrayList<ColumnSet>(tableRecordImpl.getUniqueKeys());
    	if (tableRecordImpl.getPrimaryKey() != null) {
	    	result.add(tableRecordImpl.getPrimaryKey());
    	}
    	for (KeyRecord key : tableRecordImpl.getIndexes()) {
			if (key.getType() == KeyRecord.Type.Unique) {
				result.add(key);
			}
		}
    	return result;
    }

    public Collection getForeignKeysInGroup(final Object groupID)
        throws TeiidComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	return ((Table)groupID).getForeignKeys();
    }

    public Object getPrimaryKeyIDForForeignKeyID(final Object foreignKeyID)
        throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ForeignKey.class, foreignKeyID);
        ForeignKey fkRecord = (ForeignKey) foreignKeyID;
        return fkRecord.getPrimaryKey();
    }

    public Collection getAccessPatternsInGroup(final Object groupID)
        throws TeiidComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	return ((Table)groupID).getAccessPatterns();
    }

    public List getElementIDsInIndex(final Object index) throws TeiidComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(ColumnSet.class, index);
    	return ((ColumnSet)index).getColumns();
    }

    public List getElementIDsInKey(final Object key) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ColumnSet.class, key);
        return ((ColumnSet)key).getColumns();
    }

    public List getElementIDsInAccessPattern(final Object accessPattern)
        throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ColumnSet.class, accessPattern);
        return ((ColumnSet)accessPattern).getColumns();
    }

    public boolean isXMLGroup(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);

        Table tableRecord = (Table) groupID;
        return tableRecord.getTableType() == Table.Type.Document;
    }

    /** 
     * @see org.teiid.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(final Object groupID) throws TeiidComponentException,
                                                      QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;
        return tableRecord.isMaterialized();
    }

    /** 
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(final Object groupID) throws TeiidComponentException,
                                                    QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;
        if(tableRecord.isMaterialized()) {
	        return tableRecord.getMaterializedTable();
        }
        return null;
    }

    /** 
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(final Object groupID) throws TeiidComponentException,
                                                         QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;
        if(tableRecord.isMaterialized()) {
	        return tableRecord.getMaterializedStageTable();
        }
        return null;
    }

    public MappingNode getMappingNode(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);

        Table tableRecord = (Table) groupID;
		final String groupName = tableRecord.getFullName();
        if(tableRecord.isVirtual()) {
            // get mappin transform
            String document = tableRecord.getSelectTransformation();            
            InputStream inputStream = new ByteArrayInputStream(document.getBytes());
            MappingLoader reader = new MappingLoader();
            MappingDocument mappingDoc = null;
            try{
                mappingDoc = reader.loadDocument(inputStream);
                mappingDoc.setName(groupName);
            } catch (Exception e){
                throw new TeiidComponentException(e, QueryPlugin.Util.getString("TransformationMetadata.Error_trying_to_read_virtual_document_{0},_with_body__n{1}_1", groupName, mappingDoc)); //$NON-NLS-1$
            } finally {
            	try {
					inputStream.close();
            	} catch(Exception e) {}
            }
            return (MappingDocument)mappingDoc.clone();
        }

        return null;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getVirtualDatabaseName()
     */
    public String getVirtualDatabaseName() throws TeiidComponentException, QueryMetadataException {
    	if (vdbMetaData == null) {
    		return null;
    	}
    	return vdbMetaData.getName();
    }

    public int getVirtualDatabaseVersion() {
    	if (vdbMetaData == null) {
    		return 0;
    	}
    	return vdbMetaData.getVersion();
    }
    
    public VDBMetaData getVdbMetaData() {
		return vdbMetaData;
	}
    
    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getXMLTempGroups(java.lang.Object)
     */
    public Collection getXMLTempGroups(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;

        if(tableRecord.getTableType() == Table.Type.Document) {
            return this.store.getXMLTempGroups(tableRecord);
        }
        return Collections.EMPTY_SET;
    }

    public int getCardinality(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        return ((Table) groupID).getCardinality();
    }

    public List<SQLXMLImpl> getXMLSchemas(final Object groupID) throws TeiidComponentException, QueryMetadataException {

        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;

        // lookup transformation record for the group
        String groupName = tableRecord.getFullName();

        // get the schema Paths
        List<String> schemaPaths = tableRecord.getSchemaPaths();
        
        List<SQLXMLImpl> schemas = new LinkedList<SQLXMLImpl>();
        if (schemaPaths == null) {
        	return schemas;
        }
        File f = new File(tableRecord.getResourcePath());
        String path = f.getParent();
        if (File.separatorChar != '/') {
        	path = path.replace(File.separatorChar, '/');
        }
        for (String string : schemaPaths) {
        	String parentPath = path;
        	boolean relative = false;
        	while (string.startsWith("../")) { //$NON-NLS-1$
        		relative = true;
        		string = string.substring(3);
        		parentPath = new File(parentPath).getParent();
        	}
        	SQLXMLImpl schema = null;
        	if (!relative) {
        		schema = getVDBResourceAsSQLXML(string);
        	}
        	if (schema == null) {
        		if (!parentPath.endsWith("/")) { //$NON-NLS-1$
        			parentPath += "/"; //$NON-NLS-1$
        		}
        		schema = getVDBResourceAsSQLXML(parentPath + string);
        	}
        	
        	if (schema == null) {
        		throw new QueryMetadataException(QueryPlugin.Util.getString("TransformationMetadata.Error_trying_to_read_schemas_for_the_document/table____1")+groupName);             //$NON-NLS-1$		
        	}
        	schemas.add(schema);
        }
        
        return schemas;
    }

    public String getNameInSource(final Object metadataID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        return ((AbstractMetadataRecord) metadataID).getNameInSource();
    }

    public int getElementLength(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getLength();            
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getLength();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public int getPosition(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getPosition();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getPosition();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
    public int getPrecision(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getPrecision();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getPrecision();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
    public int getRadix(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getRadix();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getRadix();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
	public String getFormat(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getFormat();
        } 
        throw createInvalidRecordTypeException(elementID);            
	}       
    
    public int getScale(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getScale();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getScale();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public int getDistinctValues(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getDistinctValues();
        } else if(elementID instanceof ProcedureParameter) {
            return -1;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public int getNullValues(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getNullValues();
        } else if(elementID instanceof ProcedureParameter) {
            return -1;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public String getNativeType(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getNativeType();
        } else if(elementID instanceof ProcedureParameter) {
            return null;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public Properties getExtensionProperties(final Object metadataID) throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) metadataID;
        Map<String, String> result = metadataRecord.getProperties();
        if (result == null) {
        	return EMPTY_PROPS;
        }
        Properties p = new Properties();
        p.putAll(result);
        return p;
    }

    /** 
     * @see org.teiid.query.metadata.BasicQueryMetadata#getBinaryVDBResource(java.lang.String)
     * @since 4.3
     */
    public byte[] getBinaryVDBResource(String resourcePath) throws TeiidComponentException, QueryMetadataException {
    	final VirtualFile f = getFile(resourcePath);
    	if (f == null) {
    		return null;
    	}
		try {
			return ObjectConverterUtil.convertToByteArray(f.openStream());
		} catch (IOException e) {
			throw new TeiidComponentException(e);
		}
    }
    
    public ClobImpl getVDBResourceAsClob(String resourcePath) {
    	final VirtualFile f = getFile(resourcePath);
    	if (f == null) {
    		return null;
    	}
		return new ClobImpl(new VirtualFileInputStreamFactory(f), -1);
    }
    
    public SQLXMLImpl getVDBResourceAsSQLXML(String resourcePath) {
    	final VirtualFile f = getFile(resourcePath);
    	if (f == null) {
    		return null;
    	}
		return new SQLXMLImpl(new VirtualFileInputStreamFactory(f));
    }
    
    public BlobImpl getVDBResourceAsBlob(String resourcePath) {
    	final VirtualFile f = getFile(resourcePath);
    	if (f == null) {
    		return null;
    	}
    	return new BlobImpl(new VirtualFileInputStreamFactory(f));
    }
    
    private VirtualFile getFile(String resourcePath) {
    	if (resourcePath == null) {
    		return null;
    	}
    	Resource r = this.vdbEntries.get(resourcePath);
    	if (r != null) {
    		return r.file;
    	}
    	return null;
    }

    /** 
     * @see org.teiid.query.metadata.BasicQueryMetadata#getCharacterVDBResource(java.lang.String)
     * @since 4.3
     */
    public String getCharacterVDBResource(String resourcePath) throws TeiidComponentException, QueryMetadataException {
    	try {
    		byte[] bytes = getBinaryVDBResource(resourcePath);
    		if (bytes == null) {
    			return null;
    		}
			return ObjectConverterUtil.convertToString(new ByteArrayInputStream(bytes));
		} catch (IOException e) {
			throw new TeiidComponentException(e);
		}
    }
    
    public CompositeMetadataStore getMetadataStore() {
    	return this.store;
    }

    /** 
     * @see org.teiid.query.metadata.BasicQueryMetadata#getVDBResourcePaths()
     * @since 4.3
     */
    public String[] getVDBResourcePaths() throws TeiidComponentException, QueryMetadataException {
    	LinkedList<String> paths = new LinkedList<String>();
    	for (Map.Entry<String, Resource> entry : this.vdbEntries.entrySet()) {
			paths.add(entry.getKey());
    	}
    	return paths.toArray(new String[paths.size()]);
    }
    
    /** 
     * @see org.teiid.query.metadata.QueryMetadataInterface#getModeledType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledType(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        Datatype record = getDatatypeRecord(elementID);
        if (record != null) {
            return record.getDatatypeID();
        }
        return null;
    }
    
    /** 
     * @see org.teiid.query.metadata.QueryMetadataInterface#getModeledBaseType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledBaseType(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        Datatype record = getDatatypeRecord(elementID);
        if (record != null) {
            return record.getBasetypeID();
        }
        return null;
    }

    /** 
     * @see org.teiid.query.metadata.QueryMetadataInterface#getModeledPrimitiveType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledPrimitiveType(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        Datatype record = getDatatypeRecord(elementID);
        if (record != null) {
            return record.getPrimitiveTypeID();
        }
        return null;
    }

    private Datatype getDatatypeRecord(final Object elementID) {
        if (elementID instanceof Column) {
            return ((Column)elementID).getDatatype();
        } else if (elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter)elementID).getDatatype();
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

	@Override
	public Object addToMetadataCache(Object metadataID, String key, Object value)
			throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        boolean groupInfo = key.startsWith(GroupInfo.CACHE_PREFIX);
        key = getCacheKey(key, (AbstractMetadataRecord)metadataID);
        if (groupInfo) {
        	return this.groupInfoCache.put(key, value); 
        }
    	return this.metadataCache.put(key, value); 
	}

	@Override
	public Object getFromMetadataCache(Object metadataID, String key)
			throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        boolean groupInfo = key.startsWith(GroupInfo.CACHE_PREFIX);
        key = getCacheKey(key, (AbstractMetadataRecord)metadataID);
        if (groupInfo) {
        	return this.groupInfoCache.get(key); 
        }
    	return this.metadataCache.get(key);
	}

	private String getCacheKey(String key, AbstractMetadataRecord record) {
		return record.getUUID() + "/" + key; //$NON-NLS-1$
	}

	@Override
	public FunctionLibrary getFunctionLibrary() {
		return this.functionLibrary;
	}
	
	@Override
	public Object getPrimaryKey(Object metadataID) {
		ArgCheck.isInstanceOf(Table.class, metadataID);
		Table table = (Table)metadataID;
		return table.getPrimaryKey();
	}
	
	@Override
	public QueryMetadataInterface getDesignTimeMetadata() {
		return new TransformationMetadata(store, functionLibrary);
	}
}