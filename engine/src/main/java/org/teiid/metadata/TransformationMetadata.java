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

package org.teiid.metadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.ColumnSet;
import org.teiid.connector.metadata.runtime.Datatype;
import org.teiid.connector.metadata.runtime.ForeignKey;
import org.teiid.connector.metadata.runtime.KeyRecord;
import org.teiid.connector.metadata.runtime.ProcedureParameter;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.Schema;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.connector.metadata.runtime.BaseColumn.NullType;
import org.teiid.connector.metadata.runtime.Column.SearchType;
import org.teiid.connector.metadata.runtime.ProcedureParameter.Type;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.metadata.runtime.api.MetadataSourceUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.mapping.xml.MappingDocument;
import com.metamatrix.query.mapping.xml.MappingLoader;
import com.metamatrix.query.mapping.xml.MappingNode;
import com.metamatrix.query.metadata.BasicQueryMetadata;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.sql.lang.SPParameter;

/**
 * Modelers implementation of QueryMetadataInterface that reads columns, groups, models etc.
 * index files for various metadata properties.
 */
public class TransformationMetadata extends BasicQueryMetadata {

    /** Delimiter character used when specifying fully qualified entity names */
    public static final char DELIMITER_CHAR = StringUtil.Constants.DOT_CHAR;
    public static final String DELIMITER_STRING = String.valueOf(DELIMITER_CHAR);
    
    // error message cached to avoid i18n lookup each time
    public static String NOT_EXISTS_MESSAGE = StringUtil.Constants.SPACE+DQPPlugin.Util.getString("TransformationMetadata.does_not_exist._1"); //$NON-NLS-1$

    private static UnmodifiableProperties EMPTY_PROPS = new UnmodifiableProperties(new Properties());
    
    private final CompositeMetadataStore store;
	private String vdbVersion;
	private VDBService vdbService;

    /*
     * TODO: move caching to jboss cache structure
     */
    private final Map<String, Object> metadataCache = Collections.synchronizedMap(new LRUCache<String, Object>(500));
    private final Map<String, Collection<Table>> partialNameToFullNameCache = Collections.synchronizedMap(new LRUCache<String, Collection<Table>>(1000));
    private final Map<String, Collection<StoredProcedureInfo>> procedureCache = Collections.synchronizedMap(new LRUCache<String, Collection<StoredProcedureInfo>>(200));
    /**
     * TransformationMetadata constructor
     * @param context Object containing the info needed to lookup metadta.
     */
    public TransformationMetadata(final CompositeMetadataStore store) {
    	this(store, null, null);
    }
    
    public TransformationMetadata(final CompositeMetadataStore store, VDBService service, String vdbVersion) {
    	ArgCheck.isNotNull(store);
        this.store = store;
        this.vdbService = service;
        this.vdbVersion = vdbVersion;
    }
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementID(java.lang.String)
     */
    public Object getElementID(final String elementName) throws MetaMatrixComponentException, QueryMetadataException {
    	int columnIndex = elementName.lastIndexOf(TransformationMetadata.DELIMITER_STRING);
		if (columnIndex == -1) {
			throw new QueryMetadataException(elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		Table table = this.store.findGroup(elementName.substring(0, columnIndex));
		String shortElementName = elementName.substring(columnIndex + 1);
		for (Column column : (List<Column>)getElementIDsInGroupID(table)) {
			if (column.getName().equalsIgnoreCase(shortElementName)) {
				return column;
			}
        }
        throw new QueryMetadataException(elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupID(java.lang.String)
     */
    public Object getGroupID(final String groupName) throws MetaMatrixComponentException, QueryMetadataException {
        return getMetadataStore().findGroup(groupName.toLowerCase());
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupsForPartialName(java.lang.String)
     */
    public Collection getGroupsForPartialName(final String partialGroupName)
        throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isNotEmpty(partialGroupName);

		Collection<Table> matches = this.partialNameToFullNameCache.get(partialGroupName);
		
		if (matches == null) {
			String partialName = DELIMITER_CHAR + partialGroupName.toLowerCase(); 
	
	        matches = getMetadataStore().getGroupsForPartialName(partialName);
	        
        	this.partialNameToFullNameCache.put(partialGroupName, matches);
		}
		
		if (matches.isEmpty()) {
			return Collections.emptyList();
		}
		
		Collection<String> filteredResult = new ArrayList<String>(matches.size());
		for (Table table : matches) {
	        if (vdbService == null || vdbService.getModelVisibility(getVirtualDatabaseName(), vdbVersion, table.getParent().getName()) == ModelInfo.PUBLIC) {
	        	filteredResult.add(table.getFullName());
	        }
		}
		return filteredResult;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModelID(java.lang.Object)
     */
    public Object getModelID(final Object groupOrElementID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(AbstractMetadataRecord.class, groupOrElementID);
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) groupOrElementID;
        AbstractMetadataRecord parent = metadataRecord.getParent();
        if (parent instanceof Schema) {
        	return parent;
        }
        if (parent == null) {
        	throw createInvalidRecordTypeException(groupOrElementID);
        }
        parent = metadataRecord.getParent();
        if (parent instanceof Schema) {
        	return parent;
        }
    	throw createInvalidRecordTypeException(groupOrElementID);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getFullName(java.lang.Object)
     */
    public String getFullName(final Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) metadataID;
        return metadataRecord.getFullName();
    }

  /* (non-Javadoc)
   * @see com.metamatrix.query.metadata.QueryMetadataInterface#getFullElementName(java.lang.String, java.lang.String)
   */
    public String getFullElementName(final String fullGroupName, final String shortElementName)     
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isNotEmpty(fullGroupName);
        ArgCheck.isNotEmpty(shortElementName);

        return fullGroupName + DELIMITER_CHAR + shortElementName;
    }

  /* (non-Javadoc)
   * @see com.metamatrix.query.metadata.QueryMetadataInterface#getShortElementName(java.lang.String)
   */
    public String getShortElementName(final String fullElementName) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isNotEmpty(fullElementName);
        int index = fullElementName.lastIndexOf(DELIMITER_CHAR);
        if(index >= 0) { 
            return fullElementName.substring(index+1);
        }
        return fullElementName;
    }

    /**
     * Return the text portion of the fullElementName representing a group.
     * That means that this should only return text that is part of the 
     * fullElementName and not look up new IDs or do much of anything fancy.
     * This method is used by the resolver to decide which portion of a fully-
     * qualified element name is the group name.  It will compare whatever comes
     * back with the actual group names and aliases in the query, which is 
     * why it is important not to introduce new metadata here.  Also, returning
     * null indicates that no portion of the fullElementName is a
     * group name - that is ok as it will be resolved as an ambiguous element.
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupName(java.lang.String)
     */
    public String getGroupName(final String fullElementName) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isNotEmpty(fullElementName);  

        int index = fullElementName.lastIndexOf(DELIMITER_CHAR);
        if(index >= 0) { 
            return fullElementName.substring(0, index);
        }
        return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInGroupID(java.lang.Object)
     */
    public List getElementIDsInGroupID(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	return ((Table)groupID).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupIDForElementID(java.lang.Object)
     */
    public Object getGroupIDForElementID(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            Column columnRecord = (Column) elementID;
            return this.getGroupID(getGroupName(columnRecord.getFullName()));
        } else if(elementID instanceof ProcedureParameter){
            ProcedureParameter columnRecord = (ProcedureParameter) elementID;
            return this.getGroupID(getGroupName(columnRecord.getFullName()));
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getStoredProcedureInfoForProcedure(java.lang.String)
     */
    public StoredProcedureInfo getStoredProcedureInfoForProcedure(final String fullyQualifiedProcedureName)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isNotEmpty(fullyQualifiedProcedureName);
        String lowerGroupName = fullyQualifiedProcedureName.toLowerCase();
        Collection<StoredProcedureInfo> results = this.procedureCache.get(lowerGroupName);
        
        if (results == null) {
        	Collection<ProcedureRecordImpl> procRecords = getMetadataStore().getStoredProcedure(lowerGroupName); 
        	results = new ArrayList<StoredProcedureInfo>(procRecords.size());
        	for (ProcedureRecordImpl procRecord : procRecords) {
                String procedureFullName = procRecord.getFullName();

                // create the storedProcedure info object that would hold procedure's metadata
                StoredProcedureInfo procInfo = new StoredProcedureInfo();
                procInfo.setProcedureCallableName(procedureFullName);
                procInfo.setProcedureID(procRecord);

                // modelID for the procedure
                procInfo.setModelID(procRecord.getSchema());

                // get the parameter metadata info
                for (ProcedureParameter paramRecord : procRecord.getParameters()) {
                    String runtimeType = paramRecord.getRuntimeType();
                    int direction = this.convertParamRecordTypeToStoredProcedureType(paramRecord.getType());
                    // create a parameter and add it to the procedure object
                    SPParameter spParam = new SPParameter(paramRecord.getPosition(), direction, paramRecord.getName());
                    spParam.setMetadataID(paramRecord);
                    spParam.setClassType(DataTypeManager.getDataTypeClass(runtimeType));
                    procInfo.addParameter(spParam);
                }

                // if the procedure returns a resultSet, obtain resultSet metadata
                if(procRecord.getResultSet() != null) {
                    ColumnSet<ProcedureRecordImpl> resultRecord = procRecord.getResultSet();
                    // resultSet is the last parameter in the procedure
                    int lastParamIndex = procInfo.getParameters().size() + 1;
                    SPParameter param = new SPParameter(lastParamIndex, SPParameter.RESULT_SET, resultRecord.getName());
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
        	this.procedureCache.put(lowerGroupName, results);        	
        }
        
        StoredProcedureInfo result = null;
        
        for (StoredProcedureInfo storedProcedureInfo : results) {
        	Schema schema = (Schema)storedProcedureInfo.getModelID();
	        if(vdbService == null || vdbService.getModelVisibility(getVirtualDatabaseName(), vdbVersion, schema.getName()) == ModelInfo.PUBLIC){
	        	if (result != null) {
	    			throw new QueryMetadataException(QueryPlugin.Util.getString("ambiguous_procedure", fullyQualifiedProcedureName)); //$NON-NLS-1$
	    		}
	        	result = storedProcedureInfo;
	        }
		}
        
		if (result == null) {
			throw new QueryMetadataException(fullyQualifiedProcedureName+NOT_EXISTS_MESSAGE);
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
            case ResultSet : return SPParameter.RESULT_SET;
            default : 
                return -1;
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementType(java.lang.Object)
     */
    public String getElementType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getRuntimeType();            
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getRuntimeType();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDefaultValue(java.lang.String)
     */
    public Object getDefaultValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getDefaultValue();            
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getDefaultValue();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMinimumValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getMinValue();            
        } else if(elementID instanceof ProcedureParameter){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMaximumValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getMaxValue();            
        } else if(elementID instanceof ProcedureParameter){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isVirtualGroup(java.lang.Object)
     */
    public boolean isVirtualGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        return ((Table) groupID).isVirtual();
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isProcedureInputElement(java.lang.Object)
     * @since 4.2
     */
    public boolean isProcedure(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
    	if(groupID instanceof ProcedureRecordImpl) {
            return true;            
        } 
    	if(groupID instanceof Table){
            return false;
        } 
    	throw createInvalidRecordTypeException(groupID);
    }

    public boolean isVirtualModel(final Object modelID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Schema.class, modelID);
        Schema modelRecord = (Schema) modelID;
        return !modelRecord.isPhysical();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getVirtualPlan(java.lang.Object)
     */
    public QueryNode getVirtualPlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);

        Table tableRecord = (Table) groupID;
        if (!tableRecord.isVirtual()) {
            throw new QueryMetadataException(DQPPlugin.Util.getString("TransformationMetadata.QueryPlan_could_not_be_found_for_physical_group__6")+tableRecord.getFullName()); //$NON-NLS-1$
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

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getInsertPlan(java.lang.Object)
     */
    public String getInsertPlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(DQPPlugin.Util.getString("TransformationMetadata.InsertPlan_could_not_be_found_for_physical_group__8")+tableRecordImpl.getFullName()); //$NON-NLS-1$
        }
        return ((Table)groupID).getInsertPlan();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getUpdatePlan(java.lang.Object)
     */
    public String getUpdatePlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
        	throw new QueryMetadataException(DQPPlugin.Util.getString("TransformationMetadata.InsertPlan_could_not_be_found_for_physical_group__10")+tableRecordImpl.getFullName());         //$NON-NLS-1$
        }
        return ((Table)groupID).getUpdatePlan();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDeletePlan(java.lang.Object)
     */
    public String getDeletePlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(DQPPlugin.Util.getString("TransformationMetadata.DeletePlan_could_not_be_found_for_physical_group__12")+tableRecordImpl.getFullName()); //$NON-NLS-1$
        }
        return ((Table)groupID).getDeletePlan();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#modelSupports(java.lang.Object, int)
     */
    public boolean modelSupports(final Object modelID, final int modelConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Schema.class, modelID);

        switch(modelConstant) {
            default:
                throw new UnsupportedOperationException(DQPPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + modelConstant); //$NON-NLS-1$
        }        
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#groupSupports(java.lang.Object, int)
     */
    public boolean groupSupports(final Object groupID, final int groupConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;

        switch(groupConstant) {
            case SupportConstants.Group.UPDATE:
                return tableRecord.supportsUpdate();
            default:
                throw new UnsupportedOperationException(DQPPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + groupConstant); //$NON-NLS-1$
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#elementSupports(java.lang.Object, int)
     */
    public boolean elementSupports(final Object elementID, final int elementConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        
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
                    return columnRecord.isAutoIncrementable();
                case SupportConstants.Element.CASE_SENSITIVE:
                    return columnRecord.isCaseSensitive();
                case SupportConstants.Element.SIGNED:
                    return columnRecord.isSigned();
                default:
                    throw new UnsupportedOperationException(DQPPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
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
                    throw new UnsupportedOperationException(DQPPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
            }
            
        } else {            
            throw createInvalidRecordTypeException(elementID);
        }
    }
    
    private IllegalArgumentException createInvalidRecordTypeException(Object elementID) {
        return new IllegalArgumentException(DQPPlugin.Util.getString("TransformationMetadata.Invalid_type", elementID.getClass().getName()));         //$NON-NLS-1$
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaxSetSize(java.lang.Object)
     */
    public int getMaxSetSize(final Object modelID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Schema.class, modelID);
        return 0;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getIndexesInGroup(java.lang.Object)
     */
    public Collection getIndexesInGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        return ((Table)groupID).getIndexes();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getUniqueKeysInGroup(java.lang.Object)
     */
    public Collection getUniqueKeysInGroup(final Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	Table tableRecordImpl = (Table)groupID;
    	ArrayList<ColumnSet> result = new ArrayList<ColumnSet>(tableRecordImpl.getUniqueKeys());
    	if (tableRecordImpl.getPrimaryKey() != null) {
	    	result.add(tableRecordImpl.getPrimaryKey());
    	}
    	for (KeyRecord key : tableRecordImpl.getIndexes()) {
			if (key.getType() == KeyRecord.Type.Index) {
				result.add(key);
			}
		}
    	return result;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getForeignKeysInGroup(java.lang.Object)
     */
    public Collection getForeignKeysInGroup(final Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	return ((Table)groupID).getForeignKeys();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getPrimaryKeyIDForForeignKeyID(java.lang.Object)
     */
    public Object getPrimaryKeyIDForForeignKeyID(final Object foreignKeyID)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ForeignKey.class, foreignKeyID);
        ForeignKey fkRecord = (ForeignKey) foreignKeyID;
        return fkRecord.getPrimaryKey();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getAccessPatternsInGroup(java.lang.Object)
     */
    public Collection getAccessPatternsInGroup(final Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(Table.class, groupID);
    	return ((Table)groupID).getAccessPatterns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInIndex(java.lang.Object)
     */
    public List getElementIDsInIndex(final Object index) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(ColumnSet.class, index);
    	return ((ColumnSet)index).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInKey(java.lang.Object)
     */
    public List getElementIDsInKey(final Object key) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ColumnSet.class, key);
        return ((ColumnSet)key).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInAccessPattern(java.lang.Object)
     */
    public List getElementIDsInAccessPattern(final Object accessPattern)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ColumnSet.class, accessPattern);
        return ((ColumnSet)accessPattern).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isXMLGroup(java.lang.Object)
     */
    public boolean isXMLGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);

        Table tableRecord = (Table) groupID;
        return tableRecord.getTableType() == Table.Type.Document;
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(final Object groupID) throws MetaMatrixComponentException,
                                                      QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;
        return tableRecord.isMaterialized();
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(final Object groupID) throws MetaMatrixComponentException,
                                                    QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;
        if(tableRecord.isMaterialized()) {
	        return tableRecord.getMaterializedTable();
        }
        return null;
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(final Object groupID) throws MetaMatrixComponentException,
                                                         QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;
        if(tableRecord.isMaterialized()) {
	        return tableRecord.getMaterializedStageTable();
        }
        return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMappingNode(java.lang.Object)
     */
    public MappingNode getMappingNode(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
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
                throw new MetaMatrixComponentException(e, DQPPlugin.Util.getString("TransformationMetadata.Error_trying_to_read_virtual_document_{0},_with_body__n{1}_1", groupName, mappingDoc)); //$NON-NLS-1$
            } finally {
            	try {
					inputStream.close();
            	} catch(Exception e) {}
            }
            return (MappingDocument)mappingDoc.clone();
        }

        return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getVirtualDatabaseName()
     */
    public String getVirtualDatabaseName() throws MetaMatrixComponentException, QueryMetadataException {
    	return this.getMetadataStore().getMetadataSource().getName();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getXMLTempGroups(java.lang.Object)
     */
    public Collection getXMLTempGroups(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;

        if(tableRecord.getTableType() == Table.Type.Document) {
            return this.store.getXMLTempGroups(tableRecord);
        }
        return Collections.EMPTY_SET;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getCardinality(java.lang.Object)
     */
    public int getCardinality(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(Table.class, groupID);
        return ((Table) groupID).getCardinality();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getXMLSchemas(java.lang.Object)
     */
    public List getXMLSchemas(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {

        ArgCheck.isInstanceOf(Table.class, groupID);
        Table tableRecord = (Table) groupID;

        // lookup transformation record for the group
        String groupName = tableRecord.getFullName();

        // get the schema Paths
        List<String> schemaPaths = tableRecord.getSchemaPaths();
        
        List<String> schemas = new LinkedList<String>();
        if (schemaPaths == null) {
        	return schemas;
        }
        File f = new File(tableRecord.getResourcePath());
        String path = f.getParent();
        
        for (String string : schemaPaths) {
        	String schema = getCharacterVDBResource(string);
        	
        	if (schema == null) {
        		schema = getCharacterVDBResource(path + File.separator + string);
        	}
        	
        	if (schema == null) {
        		throw new MetaMatrixComponentException(DQPPlugin.Util.getString("TransformationMetadata.Error_trying_to_read_schemas_for_the_document/table____1")+groupName);             //$NON-NLS-1$		
        	}
        }
        
        return schemas;
    }

    public String getNameInSource(final Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        return ((AbstractMetadataRecord) metadataID).getNameInSource();
    }

    public int getElementLength(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getLength();            
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getLength();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public int getPosition(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getPosition();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getPosition();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
    public int getPrecision(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getPrecision();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getPrecision();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
    public int getRadix(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getRadix();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getRadix();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
	public String getFormat(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getFormat();
        } 
        throw createInvalidRecordTypeException(elementID);            
	}       
    
    public int getScale(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getScale();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getScale();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public int getDistinctValues(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getDistinctValues();
        } else if(elementID instanceof ProcedureParameter) {
            return -1;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public int getNullValues(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getNullValues();
        } else if(elementID instanceof ProcedureParameter) {
            return -1;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public String getNativeType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getNativeType();
        } else if(elementID instanceof ProcedureParameter) {
            return null;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    /* 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getExtensionProperties(java.lang.Object)
     */
    public Properties getExtensionProperties(final Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
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
     * @see com.metamatrix.query.metadata.BasicQueryMetadata#getBinaryVDBResource(java.lang.String)
     * @since 4.3
     */
    public byte[] getBinaryVDBResource(String resourcePath) throws MetaMatrixComponentException,
                                                           QueryMetadataException {
        String content = this.getCharacterVDBResource(resourcePath);
        if(content != null) {
            return content.getBytes();
        }
        return null;
    }

    /** 
     * @see com.metamatrix.query.metadata.BasicQueryMetadata#getCharacterVDBResource(java.lang.String)
     * @since 4.3
     */
    public String getCharacterVDBResource(String resourcePath) throws MetaMatrixComponentException,
                                                              QueryMetadataException {
    	return MetadataSourceUtil.getFileContentAsString(resourcePath, this.getMetadataStore().getMetadataSource());
    }
    
    public CompositeMetadataStore getMetadataStore() {
    	return this.store;
    }

    /** 
     * @see com.metamatrix.query.metadata.BasicQueryMetadata#getVDBResourcePaths()
     * @since 4.3
     */
    public String[] getVDBResourcePaths() throws MetaMatrixComponentException,
                                         QueryMetadataException {
        return getMetadataStore().getMetadataSource().getEntries().toArray(new String[0]);
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        Datatype record = getDatatypeRecord(elementID);
        if (record != null) {
            return record.getDatatypeID();
        }
        return null;
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledBaseType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledBaseType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        Datatype record = getDatatypeRecord(elementID);
        if (record != null) {
            return record.getBasetypeID();
        }
        return null;
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledPrimitiveType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledPrimitiveType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
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
			throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        key = getCacheKey(key, (AbstractMetadataRecord)metadataID);
    	return this.metadataCache.put(key, value); 
	}

	@Override
	public Object getFromMetadataCache(Object metadataID, String key)
			throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        key = getCacheKey(key, (AbstractMetadataRecord)metadataID);
    	return this.metadataCache.get(key);
	}

	private String getCacheKey(String key, AbstractMetadataRecord record) {
		return record.getUUID() + "/" + key; //$NON-NLS-1$
	}

}