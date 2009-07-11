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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.ColumnRecordImpl;
import org.teiid.connector.metadata.runtime.ColumnSetRecordImpl;
import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;
import org.teiid.connector.metadata.runtime.ForeignKeyRecordImpl;
import org.teiid.connector.metadata.runtime.MetadataConstants;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureParameterRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;
import org.teiid.connector.metadata.runtime.TransformationRecordImpl;
import org.teiid.metadata.index.IndexConstants;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.metadata.runtime.api.MetadataSourceUtil;
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

	//Fix Me: The following constants come from com.metamatrix.metamodels.relational.NullableType
	private static int NULLABLE = 1;
	private static int NULLABLE_UNKNOWN = 2;
	//Fix Me: The following constants come from com.metamatrix.metamodels.relational.SearchabilityType
	private static int SEARCHABLE = 0;
	private static int ALL_EXCEPT_LIKE = 1;
	private static int LIKE_ONLY = 2;
	
    /** Delimiter character used when specifying fully qualified entity names */
    public static final char DELIMITER_CHAR = IndexConstants.NAME_DELIM_CHAR;
    public static final String DELIMITER_STRING = StringUtil.Constants.EMPTY_STRING + IndexConstants.NAME_DELIM_CHAR;
    
    // error message cached to avoid i18n lookup each time
    public static String NOT_EXISTS_MESSAGE = StringUtil.Constants.SPACE+RuntimeMetadataPlugin.Util.getString("TransformationMetadata.does_not_exist._1"); //$NON-NLS-1$

    private final CompositeMetadataStore store;

    /*
     * TODO: move caching to jboss cache structure
     */
    private final Map<String, Object> metadataCache = Collections.synchronizedMap(new LRUCache<String, Object>(500));
    private final Map<String, TableRecordImpl> groupCache = Collections.synchronizedMap(new LRUCache<String, TableRecordImpl>(2000));
    private final Map<String, StoredProcedureInfo> procedureCache = Collections.synchronizedMap(new LRUCache<String, StoredProcedureInfo>(200));
    private final Map<String, String> partialNameToFullNameCache = Collections.synchronizedMap(new LRUCache<String, String>(1000));
    private final Map<String, ModelRecordImpl> modelCache = Collections.synchronizedMap(new LRUCache<String, ModelRecordImpl>(100));
    
    /**
     * TransformationMetadata constructor
     * @param context Object containing the info needed to lookup metadta.
     */
    public TransformationMetadata(final CompositeMetadataStore store) {
    	ArgCheck.isNotNull(store);
        this.store = store;
    }

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementID(java.lang.String)
     */
    public Object getElementID(final String elementName) throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isNotEmpty(elementName);

        return getMetadataStore().findElement(elementName);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupID(java.lang.String)
     */
    public Object getGroupID(final String groupName) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isNotEmpty(groupName);
        String upperGroupName = groupName.toUpperCase();
        TableRecordImpl result = this.groupCache.get(upperGroupName);
        
        if (result == null) {
        	result = getMetadataStore().findGroup(groupName);
        	this.groupCache.put(upperGroupName, result);
        }
        return result;
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupsForPartialName(java.lang.String)
     */
    public Collection getGroupsForPartialName(final String partialGroupName)
        throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isNotEmpty(partialGroupName);

		String groupName = this.partialNameToFullNameCache.get(partialGroupName);
		
		if (groupName != null) {
			return Arrays.asList(groupName);
		}
		
		String partialName = DELIMITER_CHAR + partialGroupName.toLowerCase(); 

        Collection result = getMetadataStore().getGroupsForPartialName(partialName);
        
        if (result.size() == 1) {
        	this.partialNameToFullNameCache.put(partialGroupName, (String)result.iterator().next());
        }
        return result;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModelID(java.lang.Object)
     */
    public Object getModelID(final Object groupOrElementID) throws MetaMatrixComponentException, QueryMetadataException {
        if (!(groupOrElementID instanceof TableRecordImpl) && !(groupOrElementID instanceof ColumnRecordImpl)) {
        	throw createInvalidRecordTypeException(groupOrElementID);
        }
        
        String modelName = ((AbstractMetadataRecord)groupOrElementID).getModelName();
        return getModel(modelName);
    }

	private Object getModel(String modelName) throws QueryMetadataException,
			MetaMatrixComponentException {
		modelName = modelName.toUpperCase();
		ModelRecordImpl model = modelCache.get(modelName);
        if (model == null) {
        	model = getMetadataStore().getModel(modelName);
        	modelCache.put(modelName, model);
        }
        return model;
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
    	ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
    	return ((TableRecordImpl)groupID).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupIDForElementID(java.lang.Object)
     */
    public Object getGroupIDForElementID(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            ColumnRecordImpl columnRecord = (ColumnRecordImpl) elementID;
            return this.getGroupID(columnRecord.getParentFullName());
        } else if(elementID instanceof ProcedureParameterRecordImpl){
            ProcedureParameterRecordImpl columnRecord = (ProcedureParameterRecordImpl) elementID;
            return this.getGroupID(columnRecord.getParentFullName());
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
        String upperGroupName = fullyQualifiedProcedureName.toUpperCase();
        StoredProcedureInfo procInfo = this.procedureCache.get(upperGroupName);
        
        if (procInfo != null) {
        	return procInfo;
        }
        
    	ProcedureRecordImpl procRecord = getMetadataStore().getStoredProcedure(fullyQualifiedProcedureName); 

        String procedureFullName = procRecord.getFullName();

        // create the storedProcedure info object that would hold procedure's metadata
        procInfo = new StoredProcedureInfo();
        procInfo.setProcedureCallableName(procRecord.getName());
        procInfo.setProcedureID(procRecord);

        // modelID for the procedure
        procInfo.setModelID(getModel(procRecord.getModelName()));

        // get the parameter metadata info
        for (ProcedureParameterRecordImpl paramRecord : procRecord.getParameters()) {
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
            ColumnSetRecordImpl resultRecord = procRecord.getResultSet();
            // resultSet is the last parameter in the procedure
            int lastParamIndex = procInfo.getParameters().size() + 1;
            SPParameter param = new SPParameter(lastParamIndex, SPParameter.RESULT_SET, resultRecord.getFullName());
            param.setClassType(java.sql.ResultSet.class);           
            param.setMetadataID(resultRecord);

            for (ColumnRecordImpl columnRecord : resultRecord.getColumns()) {
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

    	this.procedureCache.put(upperGroupName, procInfo);
    	
        return procInfo;
    }
    
    /**
     * Method to convert the parameter type returned from a ProcedureParameterRecord
     * to the parameter type expected by StoredProcedureInfo
     * @param parameterType
     * @return
     */
    private int convertParamRecordTypeToStoredProcedureType(final int parameterType) {
        switch (parameterType) {
            case MetadataConstants.PARAMETER_TYPES.IN_PARM : return SPParameter.IN;
            case MetadataConstants.PARAMETER_TYPES.OUT_PARM : return SPParameter.OUT;
            case MetadataConstants.PARAMETER_TYPES.INOUT_PARM : return SPParameter.INOUT;
            case MetadataConstants.PARAMETER_TYPES.RETURN_VALUE : return SPParameter.RETURN_VALUE;
            case MetadataConstants.PARAMETER_TYPES.RESULT_SET : return SPParameter.RESULT_SET;
            default : 
                return -1;
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementType(java.lang.Object)
     */
    public String getElementType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getRuntimeType();            
        } else if(elementID instanceof ProcedureParameterRecordImpl){
            return ((ProcedureParameterRecordImpl) elementID).getRuntimeType();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDefaultValue(java.lang.String)
     */
    public Object getDefaultValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getDefaultValue();            
        } else if(elementID instanceof ProcedureParameterRecordImpl){
            return ((ProcedureParameterRecordImpl) elementID).getDefaultValue();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMinimumValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getMinValue();            
        } else if(elementID instanceof ProcedureParameterRecordImpl){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMaximumValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getMaxValue();            
        } else if(elementID instanceof ProcedureParameterRecordImpl){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isVirtualGroup(java.lang.Object)
     */
    public boolean isVirtualGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        return ((TableRecordImpl) groupID).isVirtual();
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isProcedureInputElement(java.lang.Object)
     * @since 4.2
     */
    public boolean isProcedure(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
    	if(groupID instanceof ProcedureRecordImpl) {
            return true;            
        } 
    	if(groupID instanceof TableRecordImpl){
            return false;
        } 
    	throw createInvalidRecordTypeException(groupID);
    }

    public boolean isVirtualModel(final Object modelID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ModelRecordImpl.class, modelID);
        ModelRecordImpl modelRecord = (ModelRecordImpl) modelID;
        return (modelRecord.getModelType() == ModelType.VIRTUAL);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getVirtualPlan(java.lang.Object)
     */
    public QueryNode getVirtualPlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);

        TableRecordImpl tableRecord = (TableRecordImpl) groupID;
        if (!tableRecord.isVirtual()) {
            throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.QueryPlan_could_not_be_found_for_physical_group__6")+tableRecord.getFullName()); //$NON-NLS-1$
        }
        TransformationRecordImpl select = tableRecord.getSelectTransformation();
        String transQuery = select.getTransformation();
        QueryNode queryNode = new QueryNode(tableRecord.getFullName(), transQuery);

        // get any bindings and add them onto the query node
        List bindings = select.getBindings();
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
    	ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecordImpl = (TableRecordImpl)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.InsertPlan_could_not_be_found_for_physical_group__8")+tableRecordImpl.getFullName()); //$NON-NLS-1$
        }
        return ((TableRecordImpl)groupID).getInsertPlan();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getUpdatePlan(java.lang.Object)
     */
    public String getUpdatePlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecordImpl = (TableRecordImpl)groupID;
        if (!tableRecordImpl.isVirtual()) {
        	throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.InsertPlan_could_not_be_found_for_physical_group__10")+tableRecordImpl.getFullName());         //$NON-NLS-1$
        }
        return ((TableRecordImpl)groupID).getUpdatePlan();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDeletePlan(java.lang.Object)
     */
    public String getDeletePlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecordImpl = (TableRecordImpl)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.DeletePlan_could_not_be_found_for_physical_group__12")+tableRecordImpl.getFullName()); //$NON-NLS-1$
        }
        return ((TableRecordImpl)groupID).getDeletePlan();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#modelSupports(java.lang.Object, int)
     */
    public boolean modelSupports(final Object modelID, final int modelConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ModelRecordImpl.class, modelID);

        switch(modelConstant) {
            default:
                throw new UnsupportedOperationException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + modelConstant); //$NON-NLS-1$
        }        
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#groupSupports(java.lang.Object, int)
     */
    public boolean groupSupports(final Object groupID, final int groupConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecord = (TableRecordImpl) groupID;

        switch(groupConstant) {
            case SupportConstants.Group.UPDATE:
                return tableRecord.supportsUpdate();
            default:
                throw new UnsupportedOperationException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + groupConstant); //$NON-NLS-1$
        }
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#elementSupports(java.lang.Object, int)
     */
    public boolean elementSupports(final Object elementID, final int elementConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        
        if(elementID instanceof ColumnRecordImpl) {
            ColumnRecordImpl columnRecord = (ColumnRecordImpl) elementID;            
            switch(elementConstant) {
                case SupportConstants.Element.NULL:
                    int ntype1 = columnRecord.getNullType();
                    return (ntype1 == NULLABLE);
                case SupportConstants.Element.NULL_UNKNOWN:
                    int ntype2 = columnRecord.getNullType();
                    return (ntype2 == NULLABLE_UNKNOWN);
                case SupportConstants.Element.SEARCHABLE_COMPARE:
                    int stype1 = columnRecord.getSearchType();
                    return (stype1 == SEARCHABLE || stype1 == ALL_EXCEPT_LIKE);
                case SupportConstants.Element.SEARCHABLE_LIKE:
                    int stype2 = columnRecord.getSearchType();
                    return (stype2 == SEARCHABLE || stype2 == LIKE_ONLY);
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
                    throw new UnsupportedOperationException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
            }
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
            ProcedureParameterRecordImpl columnRecord = (ProcedureParameterRecordImpl) elementID;            
            switch(elementConstant) {
                case SupportConstants.Element.NULL:
                    int ntype1 = columnRecord.getNullType();
                    return (ntype1 == NULLABLE);
                case SupportConstants.Element.NULL_UNKNOWN:
                    int ntype2 = columnRecord.getNullType();
                    return (ntype2 == NULLABLE_UNKNOWN);
                case SupportConstants.Element.SEARCHABLE_COMPARE:
                case SupportConstants.Element.SEARCHABLE_LIKE:
                    return false;
                case SupportConstants.Element.SELECT:
                    
                    if (columnRecord.getType() == MetadataConstants.PARAMETER_TYPES.IN_PARM) {
                        return false;
                    }
                    
                    return true;
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
                    throw new UnsupportedOperationException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
            }
            
        } else {            
            throw createInvalidRecordTypeException(elementID);
        }
    }
    
    private IllegalArgumentException createInvalidRecordTypeException(Object elementID) {
        return new IllegalArgumentException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Invalid_type", elementID.getClass().getName()));         //$NON-NLS-1$
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaxSetSize(java.lang.Object)
     */
    public int getMaxSetSize(final Object modelID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ModelRecordImpl.class, modelID);
        return ((ModelRecordImpl) modelID).getMaxSetSize();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getIndexesInGroup(java.lang.Object)
     */
    public Collection getIndexesInGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        return ((TableRecordImpl)groupID).getIndexes();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getUniqueKeysInGroup(java.lang.Object)
     */
    public Collection getUniqueKeysInGroup(final Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
    	TableRecordImpl tableRecordImpl = (TableRecordImpl)groupID;
    	if (tableRecordImpl.getPrimaryKey() != null) {
	    	ArrayList<ColumnSetRecordImpl> result = new ArrayList<ColumnSetRecordImpl>(tableRecordImpl.getUniqueKeys());
	    	result.add(tableRecordImpl.getPrimaryKey());
	    	return result;
    	}
    	return tableRecordImpl.getUniqueKeys();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getForeignKeysInGroup(java.lang.Object)
     */
    public Collection getForeignKeysInGroup(final Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
    	return ((TableRecordImpl)groupID).getForeignKeys();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getPrimaryKeyIDForForeignKeyID(java.lang.Object)
     */
    public Object getPrimaryKeyIDForForeignKeyID(final Object foreignKeyID)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ForeignKeyRecordImpl.class, foreignKeyID);
        ForeignKeyRecordImpl fkRecord = (ForeignKeyRecordImpl) foreignKeyID;
        return fkRecord.getPrimaryKey();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getAccessPatternsInGroup(java.lang.Object)
     */
    public Collection getAccessPatternsInGroup(final Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
    	return ((TableRecordImpl)groupID).getAccessPatterns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInIndex(java.lang.Object)
     */
    public List getElementIDsInIndex(final Object index) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(ColumnSetRecordImpl.class, index);
    	return ((ColumnSetRecordImpl)index).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInKey(java.lang.Object)
     */
    public List getElementIDsInKey(final Object key) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ColumnSetRecordImpl.class, key);
        return ((ColumnSetRecordImpl)key).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInAccessPattern(java.lang.Object)
     */
    public List getElementIDsInAccessPattern(final Object accessPattern)
        throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ColumnSetRecordImpl.class, accessPattern);
        return ((ColumnSetRecordImpl)accessPattern).getColumns();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isXMLGroup(java.lang.Object)
     */
    public boolean isXMLGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);

        TableRecordImpl tableRecord = (TableRecordImpl) groupID;
        if(tableRecord.getTableType() == MetadataConstants.TABLE_TYPES.DOCUMENT_TYPE) {
            return true;    
        }
        return false;
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(final Object groupID) throws MetaMatrixComponentException,
                                                      QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecord = (TableRecordImpl) groupID;
        return tableRecord.isMaterialized();
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(final Object groupID) throws MetaMatrixComponentException,
                                                    QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecord = (TableRecordImpl) groupID;
        if(tableRecord.isMaterialized()) {
	        return this.getGroupID(tableRecord.getMaterializedTableName());
        }
        return null;
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(final Object groupID) throws MetaMatrixComponentException,
                                                         QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecord = (TableRecordImpl) groupID;
        if(tableRecord.isMaterialized()) {
	        return this.getGroupID(tableRecord.getMaterializedStageTableName());
        }
        return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMappingNode(java.lang.Object)
     */
    public MappingNode getMappingNode(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);

        TableRecordImpl tableRecord = (TableRecordImpl) groupID;
		final String groupName = tableRecord.getFullName();
        if(tableRecord.isVirtual()) {
            // get the transform record for this group            
            TransformationRecordImpl transformRecord = null;
			// Query the index files
			Collection results = getMetadataStore().findMetadataRecords(MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM,groupName,false);
			int resultSize = results.size();
			if(resultSize == 1) {
				// get the columnset record for this result            
				transformRecord = (TransformationRecordImpl) results.iterator().next();            
			} else {
				if(resultSize == 0) {
					throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Could_not_find_transformation_record_for_the_group__1")+groupName); //$NON-NLS-1$
				}
				// there should be only one for a fully qualified elementName            
				if(resultSize > 1) {
					throw new MetaMatrixComponentException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Multiple_transformation_records_found_for_the_group___1")+groupName); //$NON-NLS-1$
				}
			}
            // get mappin transform
            String document = transformRecord.getTransformation();            
            InputStream inputStream = new ByteArrayInputStream(document.getBytes());
            MappingLoader reader = new MappingLoader();
            MappingDocument mappingDoc = null;
            try{
                mappingDoc = reader.loadDocument(inputStream);
                mappingDoc.setName(groupName);
            } catch (Exception e){
                throw new MetaMatrixComponentException(e, RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Error_trying_to_read_virtual_document_{0},_with_body__n{1}_1", groupName, mappingDoc)); //$NON-NLS-1$
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
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecord = (TableRecordImpl) groupID;

        int tableType = tableRecord.getTableType();
        if(tableType == MetadataConstants.TABLE_TYPES.DOCUMENT_TYPE) {
			return getMetadataStore().getXMLTempGroups(tableRecord);
        }
        return Collections.EMPTY_SET;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getCardinality(java.lang.Object)
     */
    public int getCardinality(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        return ((TableRecordImpl) groupID).getCardinality();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getXMLSchemas(java.lang.Object)
     */
    public List getXMLSchemas(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {

        ArgCheck.isInstanceOf(TableRecordImpl.class, groupID);
        TableRecordImpl tableRecord = (TableRecordImpl) groupID;

        // lookup transformation record for the group
        String groupName = tableRecord.getFullName();
		TransformationRecordImpl transformRecord = null;

		// Query the index files
		Collection results = getMetadataStore().findMetadataRecords(MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM,groupName,false);
		int resultSize = results.size();
		if(resultSize == 1) {
			// get the columnset record for this result            
			transformRecord = (TransformationRecordImpl) results.iterator().next();            
		} else {
			if(resultSize == 0) {
				throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Could_not_find_transformation_record_for_the_group__1")+groupName); //$NON-NLS-1$
			}
			// there should be only one for a fully qualified elementName            
			if(resultSize > 1) {
				throw new MetaMatrixComponentException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Multiple_transformation_records_found_for_the_group___1")+groupName); //$NON-NLS-1$
			}
		}

        // get the schema Paths
        List<String> schemaPaths = transformRecord.getSchemaPaths();
        
        List<String> schemas = new LinkedList<String>();
        
        File f = new File(transformRecord.getResourcePath());
        String path = f.getParent();
        
        for (String string : schemaPaths) {
        	String schema = getCharacterVDBResource(string);
        	
        	if (schema == null) {
        		schema = getCharacterVDBResource(path + File.separator + string);
        	}
        	
        	if (schema == null) {
        		throw new MetaMatrixComponentException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Error_trying_to_read_schemas_for_the_document/table____1")+groupName);             //$NON-NLS-1$		
        	}
        }
        
        return schemas;
    }

    public String getNameInSource(final Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(AbstractMetadataRecord.class, metadataID);
        return ((AbstractMetadataRecord) metadataID).getNameInSource();
    }

    public int getElementLength(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getLength();            
        } else if(elementID instanceof ProcedureParameterRecordImpl){
            return ((ProcedureParameterRecordImpl) elementID).getLength();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public int getPosition(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getPosition();
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
            return ((ProcedureParameterRecordImpl) elementID).getPosition();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
    public int getPrecision(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getPrecision();
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
            return ((ProcedureParameterRecordImpl) elementID).getPrecision();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
    public int getRadix(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getRadix();
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
            return ((ProcedureParameterRecordImpl) elementID).getRadix();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }
    
	public String getFormat(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getFormat();
        } 
        throw createInvalidRecordTypeException(elementID);            
	}       
    
    public int getScale(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getScale();
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
            return ((ProcedureParameterRecordImpl) elementID).getScale();            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public int getDistinctValues(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getDistinctValues();
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
            return -1;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public int getNullValues(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getNullValues();
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
            return -1;            
        } else {
            throw createInvalidRecordTypeException(elementID);            
        }
    }

    public String getNativeType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl) elementID).getNativeType();
        } else if(elementID instanceof ProcedureParameterRecordImpl) {
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
        if (metadataRecord.getProperties() == null) {
        	Properties p = new Properties();
        	Collection<PropertyRecordImpl> props = getMetadataStore().getExtensionProperties(metadataRecord);
        	for (PropertyRecordImpl propertyRecordImpl : props) {
				p.setProperty(propertyRecordImpl.getPropertyName(), propertyRecordImpl.getPropertyValue());
			}
            metadataRecord.setProperties(p);
        }
        return metadataRecord.getProperties();
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
    
    protected CompositeMetadataStore getMetadataStore() {
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
        DatatypeRecordImpl record = getDatatypeRecord(elementID);
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
        DatatypeRecordImpl record = getDatatypeRecord(elementID);
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
        DatatypeRecordImpl record = getDatatypeRecord(elementID);
        if (record != null) {
            return record.getPrimitiveTypeID();
        }
        return null;
    }

    private DatatypeRecordImpl getDatatypeRecord(final Object elementID) {
        if (elementID instanceof ColumnRecordImpl) {
            return ((ColumnRecordImpl)elementID).getDatatype();
        } else if (elementID instanceof ProcedureParameterRecordImpl) {
            return ((ProcedureParameterRecordImpl)elementID).getDatatype();
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
		return record.getRecordType() + "/" + record.getFullName() + "/" + key; //$NON-NLS-1$ //$NON-NLS-2$
	}

}