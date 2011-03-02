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

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingNode;


/**
 * This interface defines the way that query components access metadata.  Any
 * user of a query component will need to implement this interface.  Many
 * of these methods take or return things of type "Object".  Typically, these 
 * objects represent a metadata-implementation-specific metadata ID.  
 */
public interface QueryMetadataInterface {
	
	/**
	 * Unknown cardinality.
	 */
	int UNKNOWN_CARDINALITY = 0;
	
    /**
     * Get the metadata-implementation identifier object for the given element name.  
     * @param elementName Fully qualified element name
     * @return Metadata identifier for this element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Object getElementID(String elementName)
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the metadata-implementation identifier object for the given group name.  
     * @param groupName Fully qualified group name
     * @return Metadata identifier for this group
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Object getGroupID(String groupName)
        throws TeiidComponentException, QueryMetadataException;
       
    /**
     * Get a collection of group names that match the partially qualified group name.
     * @param partialGroupName Partially qualified group name
     * @return A collection of groups whose names are matched by the partial name.
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */        
    Collection getGroupsForPartialName(String partialGroupName)
        throws TeiidComponentException, QueryMetadataException;
        
    /**
     * Get the metadata-implementation identifier object for the model containing the 
     * specified group or element ID.
     * @param groupOrElementID Metadata group or element ID 
     * @return Metadata identifier for the model
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Object getModelID(Object groupOrElementID)
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the fully qualified (unique) name of the metadata identifier specified.  This metadata
     * identifier was previously returned by some other method.
     * @param metadataID Metadata identifier
     * @return Metadata identifier for this model
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    String getFullName(Object metadataID)
        throws TeiidComponentException, QueryMetadataException;    

    /**
     * Get the fully qualified (unique) element name given the full group name and the short element name.  
     * @param fullGroupName Fully qualified group name
     * @param shortElementName Short element name
     * @return Unique element name for the specified group and element name
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    String getFullElementName(String fullGroupName, String shortElementName)     
        throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Get the short (unique only to group) name of an element, given a fully qualified element name.
     * @param fullElementName Fully qualified element name
     * @return Short (unique only to group) name of element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    String getShortElementName(String fullElementName) 
        throws TeiidComponentException, QueryMetadataException;    

    /**
     * Get the group name from a fully qualified element name.
     * @param fullElementName Fully qualified element name
     * @return Group name
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    String getGroupName(String fullElementName) 
        throws TeiidComponentException, QueryMetadataException;    
        
	/**
	 * Get list of metadata element IDs for a group ID
	 * @param groupID Group ID
	 * @return List of Object, where each object is a metadata elementID for element within group
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    List getElementIDsInGroupID(Object groupID)
        throws TeiidComponentException, QueryMetadataException;

	/**
	 * Get containg group ID given element ID
	 * @param elementID Element ID
	 * @return Group ID containing elementID
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    Object getGroupIDForElementID(Object elementID)
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the the StoredProcedureInfo based on the fully qualified procedure name
     * @param fullyQualifiedProcedureName the fully qualified stored procedure name
     * @return StoredProcedureInfo containing the runtime model id
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    StoredProcedureInfo getStoredProcedureInfoForProcedure(String fullyQualifiedProcedureName)
        throws TeiidComponentException, QueryMetadataException;
	
    /**
     * Get the element type name for an element symbol.  These types are defined in 
     * {@link org.teiid.core.types.DataTypeManager.DefaultDataTypes}.
     * @param symbol The element symbol
     * @return The element data type
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
	String getElementType(Object elementID)
		throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the element's default value for an element symbol
     * @param elementID The element ID
     * @return The default value of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Object getDefaultValue(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the element's minimum value for an element symbol
     * @param elementID The element ID
     * @return The minimum value of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Object getMinimumValue(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the element's default value for an element symbol
     * @param elementID The element ID
     * @return The maximum value of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Object getMaximumValue(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the element's position in the group
     * @param elementID The element ID
     * @return The position of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    int getPosition(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the element's precision
     * @param elementID The element ID
     * @return The precision of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    int getPrecision(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the element's scale
     * @param elementID The element ID
     * @return The scale of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    int getScale(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the element's radix
     * @param elementID The element ID
     * @return The radix of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    int getRadix(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        
    
    /**
     * Get the element's format
     * @param elementID The element ID
     * @return The format of the element
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    String getFormat(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the number of distinct values for this column.  Negative values (typically -1)
     * indicate that the NDV is unknown.  Only applicable for physical columns.
     * @param elementID The element ID
     * @return The number of distinct values of this element in the data source
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    int getDistinctValues(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

    /**
     * Get the number of distinct values for this column.  Negative values (typically -1)
     * indicate that the NDV is unknown.  Only applicable for physical columns.
     * @param elementID The element ID
     * @return The number of distinct values of this element in the data source
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    int getNullValues(Object elementID)
        throws TeiidComponentException, QueryMetadataException;        

	/**
	 * Determine whether a group is virtual or not.
	 * @param symbol Group symbol
	 * @return True if virtual
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    boolean isVirtualGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Determine whether a model is virtual or not.
     * @param symbol model symbol
     * @return True if virtual
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    boolean isVirtualModel(Object modelID)
        throws TeiidComponentException, QueryMetadataException;

	/**
	 * Get virtual plan for a group symbol.
	 * @param symbol Group
	 * @return Root of tree of QueryNode objects
	 */
    QueryNode getVirtualPlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException;
        
	/**
	 * Get procedure defining the insert plan for this group.
	 * @param symbol Group
	 * @return A string giving the procedure for inserts.
	 */
    String getInsertPlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException;
        
	/**
	 * Get procedure defining the update plan for this group.
	 * @param symbol Group
	 * @return A string giving the procedure for inserts.
	 */
    String getUpdatePlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException;
        
	/**
	 * Get procedure defining the delete plan for this group.
	 * @param symbol Group
	 * @return A string giving the procedure for inserts.
	 */
    String getDeletePlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException;                
        
	/**
	 * Determine whether the specified model supports some feature.  
	 * @param modelID Metadata identifier specifying the model
	 * @param modelConstant Constant from {@link SupportConstants.Model}
	 * @return True if model supports feature
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    boolean modelSupports(Object modelID, int modelConstant)
        throws TeiidComponentException,QueryMetadataException;

	/**
	 * Determine whether the specified group supports some feature.  
	 * @param groupID Group metadata ID 
	 * @param groupConstant Constant from {@link SupportConstants.Group}
	 * @return True if group supports feature
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    boolean groupSupports(Object groupID, int groupConstant)
        throws TeiidComponentException,QueryMetadataException;

	/**
	 * Determine whether the specified element supports some feature.  
	 * @param elementID Element metadata ID
	 * @param elementConstant Constant from {@link SupportConstants.Element}
	 * @return True if element supports feature
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    boolean elementSupports(Object elementID, int elementConstant)
        throws TeiidComponentException,QueryMetadataException;

    /**
     * Get all extension properties defined on this metadata object  
     * @param metadataID Typically element, group, model, or procedure
     * @return All extension properties for this object or null for none
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Properties getExtensionProperties(Object metadataID) 
        throws TeiidComponentException, QueryMetadataException;

	/**
	 * Get the max set size for the specified model.
	 * @param modelID Metadata identifier specifying model
	 * @return Maximum set size
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    int getMaxSetSize(Object modelID) 
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the indexes for the specified group 
     * @param groupID Metadata identifier specifying group
     * @return Collection of Object (never null), each object representing an index
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Collection getIndexesInGroup(Object groupID) 
        throws TeiidComponentException, QueryMetadataException;
        	
	/**
	 * Get the unique keys for the specified group (primary and unique keys)
	 * @param groupID Metadata identifier specifying group
	 * @return Collection of Object (never null), each object representing a unique key
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    Collection getUniqueKeysInGroup(Object groupID) 
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the foreign keys for the specified group 
     * @param groupID Metadata identifier specifying group
     * @return Collection of Object (never null), each object representing a key
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Collection getForeignKeysInGroup(Object groupID) 
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the corresponding primary key ID for the specified foreign
     * key ID 
     * @param foreignKeyID Metadata identifier of a foreign key
     * @return Metadata ID of the corresponding primary key
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Object getPrimaryKeyIDForForeignKeyID(Object foreignKeyID)
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the access patterns for the specified group
     * @param groupID Metadata identifier specifying group
     * @return Collection of Object (never null), each object representing an access pattern
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    Collection getAccessPatternsInGroup(Object groupID) 
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the elements in the index
     * @param index Index identifier, as returned by {@link #getIndexesInGroup}
     * @return List of Object, where each object is a metadata element identifier
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    List getElementIDsInIndex(Object index) 
        throws TeiidComponentException, QueryMetadataException;
    
	/**
	 * Get the elements in the key
	 * @param key Key identifier, as returned by {@link #getUniqueKeysInGroup}
	 * @return List of Object, where each object is a metadata element identifier
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
	 */
    List getElementIDsInKey(Object key) 
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the elements in the access pattern
     * @param accessPattern access pattern identifier, as returned by {@link #getAccessPatternsInGroup}
     * @return List of Object, where each object is a metadata element identifier
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    List getElementIDsInAccessPattern(Object accessPattern) 
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Determine whether a group is an XML virtual document.
     * @param groupID Group to check 
     * @return True if group is an XML virtual document
     */
    boolean isXMLGroup(Object groupID) 
        throws TeiidComponentException, QueryMetadataException;
        
    /**
     * Get the mapping node for the given groupID
     * @param groupID Metadata group ID 
     * @return A MappingNode defining the group
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    MappingNode getMappingNode(Object groupID)
	   throws TeiidComponentException, QueryMetadataException;     

    /**
     * Get the currently connected virtual database name.  If the current metadata is not
     * virtual-database specific, then null should be returned.
     * @return Name of current virtual database
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    String getVirtualDatabaseName() 
        throws TeiidComponentException, QueryMetadataException ;
        
    /**
     * Return a list of all the temp groups used in this document.
     * @param groupID XML virtual document groupID 
     * @return List of all the temp groups used in this document.
     */
    Collection getXMLTempGroups(Object groupID) 
        throws TeiidComponentException, QueryMetadataException;
       
   /**
    * Return the cardinality for this group
    * @param groupID Metadata identifier specifying group
    * @return cardinality for the given group. If unknown, return UNKNOWN_CARDINALITY. 
    */ 
   int getCardinality(Object groupID) 
    	throws TeiidComponentException, QueryMetadataException;
        
   /**
    * Get XML schemas for a document group.
    * @param groupID Document group ID
    * @return List of String where each string is an XML schema for the document
    */
   List getXMLSchemas(Object groupID)
    throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Get the name in source of the metadata identifier specified. This metadata
     * identifier was previously returned by some other method.
     * @param metadataID Metadata identifier
     * @return Name in source as a string.
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    String getNameInSource(Object metadataID)
        throws TeiidComponentException, QueryMetadataException;          
    
    /**
     * Get the element length for a given element ID.  These types are defined in 
     * {@link org.teiid.core.types.DataTypeManager.DefaultDataTypes}.
     * @param elementID The element ID
     * @return The element length
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    int getElementLength(Object elementID)
        throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Determine whether given virtual group has an associated <i>Materialization</i>.
     * A Materialization is a cached version of the representation of a virtual group. 
     * @param groupID the groupID of the virtual group in question. 
     * @return True if given virtual group has been marked as having a Materialization.
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @since 4.2
     */
    boolean hasMaterialization(Object groupID)
        throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Accquire the physical group ID (the <i>Materialization</i>) for the given virtual
     * group ID, or <code>null</code> if the given virtual group has no Materialization.  
     * @param groupID the groupID of a virtual group that has a Materialization.
     * @return The groupID of the physical group that is a Materialization of the given virtual group.
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @since 4.2
     */
    Object getMaterialization(Object groupID)
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Accquire the physical group ID that is used for the staging area for loading
     * (the <i>Materialization</i>) for the given virtual group ID, or <code>null</code>
     * if the given virtual group has no Materialization.  
     * @param groupID the groupID of a virtual group that has a Materialization.
     * @return The groupID of the physical group that is the staging table for loading
     * the Materialization of the given virtual group.
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @since 4.2
     */
    Object getMaterializationStage(Object groupID)
        throws TeiidComponentException, QueryMetadataException;

    /**
     * Get the native type of the element specified. This element
     * identifier was previously returned by some other method.
     * @param elementID Element identifier
     * @return Native type name
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @since 4.2
     */
    String getNativeType(Object elementID)
        throws TeiidComponentException, QueryMetadataException;          

    /**
     * Get the design-time model type of the element specified. This element
     * identifier was previously returned by some other method.
     * @param elementID Element identifier
     * @return Modeled type name
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @since 5.0
     */
    String getModeledType(Object elementID)
        throws TeiidComponentException, QueryMetadataException;          

    /**
     * Get the design-time model base type of the element specified. This element
     * identifier was previously returned by some other method.
     * @param elementID Element identifier
     * @return Modeled base type name
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @since 5.0
     */
    String getModeledBaseType(Object elementID)
        throws TeiidComponentException, QueryMetadataException;          

    /**
     * Get the design-time model primitive type of the element specified. This element
     * identifier was previously returned by some other method.
     * @param elementID Element identifier
     * @return Modeled primitive type name
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @since 5.0
     */
    String getModeledPrimitiveType(Object elementID)
        throws TeiidComponentException, QueryMetadataException;          

    /**
     * Determine whether this is a procedure
     * @param groupID Group identifier
     * @return True if it is an procedure; false otherwise
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @throws TeiidComponentException Unexpected internal system problem during request
     */
    boolean isProcedure(Object groupID)
    	throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Gets the resource paths of all the resources in the VDB. 
     * @return an array of resource paths of the resources in the VDB
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @since 4.3
     */
    String[] getVDBResourcePaths()
        throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Gets the contents of a VDB resource as a String.
     * @param resourcePath a path returned by getVDBResourcePaths()
     * @return the contents of the resource as a String.
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @since 4.3
     */
    String getCharacterVDBResource(String resourcePath)
        throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Gets the contents of a VDB resource in binary form.
     * @param resourcePath a path returned by getVDBResourcePaths()
     * @return the binary contents of the resource in a byte[]
     * @throws TeiidComponentException Unexpected internal system problem during request
     * @throws QueryMetadataException Metadata implementation detected a problem during the request
     * @since 4.3
     */
    byte[] getBinaryVDBResource(String resourcePath)
        throws TeiidComponentException, QueryMetadataException;
    
    /**
     * Determine whether a group is a temporary table.
     * @param groupID Group to check 
     * @return True if group is a temporary group
     */
    boolean isTemporaryTable(Object groupID) 
        throws TeiidComponentException, QueryMetadataException;
    
    Object addToMetadataCache(Object metadataID, String key, Object value)
    	throws TeiidComponentException, QueryMetadataException;
    
    Object getFromMetadataCache(Object metadataID, String key)
    	throws TeiidComponentException, QueryMetadataException;
    
    boolean isScalarGroup(Object groupID) 
    	throws TeiidComponentException, QueryMetadataException;

    FunctionLibrary getFunctionLibrary();
    
    Object getPrimaryKey(Object metadataID);
    
    boolean isMultiSource(Object modelId) throws QueryMetadataException, TeiidComponentException;
    
    boolean isMultiSourceElement(Object elementId) throws QueryMetadataException, TeiidComponentException;

	QueryMetadataInterface getDesignTimeMetadata();
	
	boolean hasProcedure(String name) throws TeiidComponentException;
}
