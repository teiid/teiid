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

package com.metamatrix.query.metadata;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.mapping.xml.MappingNode;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 * This is an abstract implementation of the metadata interface.  It can 
 * be subclassed to create test implementations or partial implementations.
 */
public class BasicQueryMetadata implements QueryMetadataInterface {
	
    /**
     * Constructor for AbstractQueryMetadata.
     */
    public BasicQueryMetadata() {
        super();
    }

    /**
     * @see QueryMetadataInterface#getElementID(String)
     */
    public Object getElementID(String elementName)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getGroupID(String)
     */
    public Object getGroupID(String groupName)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
    /**
     * @see QueryMetadataInterface#getGroupID(String)
     */
    public Collection getGroupsForPartialName(String partialGroupName)
        throws MetaMatrixComponentException, QueryMetadataException {
		return Collections.EMPTY_LIST;        	
    }

    /**
     * @see QueryMetadataInterface#getModelID(Object)
     */
    public Object getModelID(Object groupOrElementID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getFullName(Object)
     */
    public String getFullName(Object metadataID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getFullElementName(String, String)
     */
    public String getFullElementName(
        String fullGroupName,
        String shortElementName)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getShortElementName(String)
     */
    public String getShortElementName(String fullElementName)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getGroupName(String)
     */
    public String getGroupName(String fullElementName)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getElementIDsInGroupID(Object)
     */
    public List getElementIDsInGroupID(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see QueryMetadataInterface#getGroupIDForElementID(Object)
     */
    public Object getGroupIDForElementID(Object elementID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getStoredProcedureInfoForProcedure(String)
     */
    public StoredProcedureInfo getStoredProcedureInfoForProcedure(String fullyQualifiedProcedureName)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getElementType(ElementSymbol)
     */
    public String getElementType(Object elementID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    public Object getDefaultValue(Object elementID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
	}    

    /* 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaximumValue(java.lang.Object)
     */
    public Object getMaximumValue(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /* 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMinimumValue(java.lang.Object)
     */
    public Object getMinimumValue(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDistinctValues(java.lang.Object)
     * @since 4.3
     */
    public int getDistinctValues(Object elementID) throws MetaMatrixComponentException,
                                                  QueryMetadataException {
        return -1;
    }
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getNullValues(java.lang.Object)
     * @since 4.3
     */
    public int getNullValues(Object elementID) throws MetaMatrixComponentException,
                                              QueryMetadataException {
        return -1;
    }
    
    public int getPosition(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return 0;
    }
    
    public int getPrecision(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return 0;
    }
    
    public int getRadix(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return 0;
    }

	@Override
	public String getFormat(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
		return null;
	}    
    
    public int getScale(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return 0;
    }
    

    /**
     * @see QueryMetadataInterface#isVirtualGroup(Object)
     */
    public boolean isVirtualGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(Object groupID) 
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(Object groupID) 
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(Object groupID) 
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
    /**
     * @see QueryMetadataInterface#isVirtualModel(Object)
     */
    public boolean isVirtualModel(Object modelID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#getVirtualPlan(GroupSymbol)
     */
    public QueryNode getVirtualPlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
	/**
	 * Get procedure defining the insert plan for this group.
	 * @param symbol Group
	 * @return A string giving the procedure for inserts.
	 */
    public String getInsertPlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
        
	/**
	 * Get procedure defining the update plan for this group.
	 * @param symbol Group
	 * @return A string giving the procedure for inserts.
	 */
    public String getUpdatePlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
        
	/**
	 * Get procedure defining the delete plan for this group.
	 * @param symbol Group
	 * @return A string giving the procedure for inserts.
	 */
    public String getDeletePlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#modelSupports(Object, int)
     */
    public boolean modelSupports(Object modelID, int modelConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#groupSupports(Object, int)
     */
    public boolean groupSupports(Object groupID, int groupConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#elementSupports(Object, int)
     */
    public boolean elementSupports(Object elementID, int elementConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#getMaxSetSize(Object)
     */
    public int getMaxSetSize(Object modelID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return 0;
    }


    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getIndexesInGroup(java.lang.Object)
     */
    public Collection getIndexesInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see QueryMetadataInterface#getUniqueKeysInGroup(Object)
     */
    public Collection getUniqueKeysInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see QueryMetadataInterface#getForeignKeysInGroup(Object)
     */
    public Collection getForeignKeysInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see QueryMetadataInterface#getPrimaryKeyIDForForeignKeyID(Object)
     */
    public Object getPrimaryKeyIDForForeignKeyID(Object foreignKeyID)
        throws MetaMatrixComponentException, QueryMetadataException{
        return null;
    }

    /**
     * @see QueryMetadataInterface#getElementIDsInKey(Object)
     */
    public List getElementIDsInKey(Object key)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInIndex(java.lang.Object)
     */
    public List getElementIDsInIndex(Object index)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getAccessPatternsInGroup(Object)
     */
    public Collection getAccessPatternsInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInAccessPattern(Object)
     */
    public List getElementIDsInAccessPattern(Object accessPattern)
        throws MetaMatrixComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see QueryMetadataInterface#isXMLGroup(Object)
     */
    public boolean isXMLGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#getMappingNode(GroupSymbol)
     */
    public MappingNode getMappingNode(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getVirtualDatabaseName()
     */
    public String getVirtualDatabaseName() 
        throws MetaMatrixComponentException, QueryMetadataException {
            
        return null;
    }
    
    public Collection getXMLTempGroups(Object groupID) 
        throws MetaMatrixComponentException, QueryMetadataException{
    	
    	return Collections.EMPTY_SET;    	
    }
    
    public int getCardinality(Object groupID) 
    	throws MetaMatrixComponentException, QueryMetadataException{
    		
    	return QueryMetadataInterface.UNKNOWN_CARDINALITY;
    }

    /* 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getXMLSchemas(java.lang.Object)
     */
    public List getXMLSchemas(Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getNameInSource(java.lang.Object)
     */
    public String getNameInSource(Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementLength(java.lang.Object)
     */
    public int getElementLength(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return 0;
    }

    /* 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getExtensionProperties(java.lang.Object)
     */
    public Properties getExtensionProperties(Object metadataID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    public String getNativeType(Object elementID) throws MetaMatrixComponentException,
                                                 QueryMetadataException {
        return null;
    }

	public boolean isProcedure(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
		return false;
	}
    
    public byte[] getBinaryVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    public String getCharacterVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    public String[] getVDBResourcePaths() throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledType(Object elementID) throws MetaMatrixComponentException,
                                                  QueryMetadataException {
        return null;
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledBaseType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledBaseType(Object elementID) throws MetaMatrixComponentException,
                                                      QueryMetadataException {
        return null;
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledPrimitiveType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledPrimitiveType(Object elementID) throws MetaMatrixComponentException,
                                                      QueryMetadataException {
        return null;
    }
   
    public boolean isTemporaryTable(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return false;
    }

	public Object addToMetadataCache(Object metadataID, String key, Object value)
			throws MetaMatrixComponentException, QueryMetadataException {
		return null;
	}

	public Object getFromMetadataCache(Object metadataID, String key)
			throws MetaMatrixComponentException, QueryMetadataException {
		return null;
	}

	public boolean isScalarGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return false;
	}
    
}
