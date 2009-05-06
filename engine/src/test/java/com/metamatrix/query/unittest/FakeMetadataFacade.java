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

package com.metamatrix.query.unittest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.mapping.xml.MappingBaseNode;
import com.metamatrix.query.mapping.xml.MappingDocument;
import com.metamatrix.query.mapping.xml.MappingNode;
import com.metamatrix.query.mapping.xml.MappingVisitor;
import com.metamatrix.query.mapping.xml.Navigator;
import com.metamatrix.query.metadata.BasicQueryMetadata;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.symbol.ElementSymbol;

public class FakeMetadataFacade extends BasicQueryMetadata {

	private FakeMetadataStore store;

	public FakeMetadataFacade(FakeMetadataStore store) {
		this.store = store;
	}

	public FakeMetadataStore getStore() {
		return this.store;
	}

    public Object getElementID(String elementName)
        throws MetaMatrixComponentException, QueryMetadataException {

        Assertion.isNotNull(elementName);

		Object obj = store.findObject(elementName, FakeMetadataObject.ELEMENT);
		if(obj == null) {
			throw new QueryMetadataException("Element '" + elementName + "' not found."); //$NON-NLS-1$ //$NON-NLS-2$
		}
		return obj;
	}

    public Object getGroupID(String groupName)
        throws MetaMatrixComponentException, QueryMetadataException {

        Assertion.isNotNull(groupName);

        Object result = store.findObject(groupName, FakeMetadataObject.GROUP);
        if (result == null){
            throw new QueryMetadataException("Group '" + groupName + "' not found."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return result;
	}

    public Collection getGroupsForPartialName(String partialGroupName)
        throws MetaMatrixComponentException, QueryMetadataException {

		if(partialGroupName == null) {
			throw new QueryMetadataException("Group name cannot be null"); //$NON-NLS-1$
		}
        
        String qualifiedPartialPart = "."+partialGroupName; //$NON-NLS-1$
        
		// get all groupNames present in metadata
        Collection groupNames = store.findObjects(FakeMetadataObject.GROUP, "dummy", null); //$NON-NLS-1$

		// these are the correct group names whose valid partially qualified
		// part this partial name is part of
        Collection correctGroups = new ArrayList();
        Iterator groupIter = groupNames.iterator();
        while(groupIter.hasNext()) {
        	String groupName = ((FakeMetadataObject) groupIter.next()).getName();
        	if(groupName.toLowerCase().endsWith(qualifiedPartialPart.toLowerCase())) {
        		correctGroups.add(groupName);
        	}
        }

        return correctGroups;
    }

    public Object getModelID(Object groupOrElementID)
        throws MetaMatrixComponentException, QueryMetadataException {

    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupOrElementID);
    	FakeMetadataObject obj = (FakeMetadataObject) groupOrElementID;
		return obj.getProperty(FakeMetadataObject.Props.MODEL);
	}

    public String getInsertPlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
		FakeMetadataObject obj = (FakeMetadataObject) groupID;
		return (String) obj.getProperty(FakeMetadataObject.Props.INSERT_PROCEDURE);
    }

    public String getUpdatePlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
		FakeMetadataObject obj = (FakeMetadataObject) groupID;
		return (String) obj.getProperty(FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }

    public String getDeletePlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
		FakeMetadataObject obj = (FakeMetadataObject) groupID;
		return (String) obj.getProperty(FakeMetadataObject.Props.DELETE_PROCEDURE);
    }

    public String getFullName(Object metadataID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, metadataID);
		return ((FakeMetadataObject)metadataID).getName();
	}

    public List getElementIDsInGroupID(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
		List ids = store.findObjects(FakeMetadataObject.ELEMENT, FakeMetadataObject.Props.GROUP, groupID);
		Collections.sort(ids);
		return ids;
	}

    public Object getGroupIDForElementID(Object elementID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
		FakeMetadataObject element = (FakeMetadataObject) elementID;
		return element.getProperty(FakeMetadataObject.Props.GROUP);
	}

	public String getElementType(Object elementID)
		throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element = (FakeMetadataObject) elementID;
		return (String) element.getProperty(FakeMetadataObject.Props.TYPE);
	}

	public Object getDefaultValue(Object elementID)
		throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
		return ((FakeMetadataObject)elementID).getDefaultValue();
	}

    public boolean isVirtualGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
    	FakeMetadataObject group = (FakeMetadataObject) groupID;
		return Boolean.TRUE.equals(group.getProperty(FakeMetadataObject.Props.IS_VIRTUAL));
	}

    public boolean isVirtualModel(Object modelID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, modelID);
    	FakeMetadataObject model = (FakeMetadataObject) modelID;
        return ((Boolean)model.getProperty(FakeMetadataObject.Props.IS_VIRTUAL)).booleanValue();

    }

    public QueryNode getVirtualPlan(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
    	FakeMetadataObject group = (FakeMetadataObject) groupID;
		QueryNode queryNode = (QueryNode) group.getProperty(FakeMetadataObject.Props.PLAN);
		if (queryNode.getQuery() == null) {
		    throw new QueryMetadataException("no query");
		}
		return queryNode;
	}

	public boolean modelSupports(Object modelID, int supportConstant)
        throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isInstanceOf(FakeMetadataObject.class, modelID);
		switch(supportConstant) {
			default:
				throw new QueryMetadataException("Unknown model support constant: " + supportConstant); //$NON-NLS-1$
		}
	}

    public boolean groupSupports(Object groupID, int groupConstant)
        throws MetaMatrixComponentException,QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
    	FakeMetadataObject group = (FakeMetadataObject) groupID;
		Boolean supports = null;
		switch(groupConstant) {
			case SupportConstants.Group.UPDATE:
				supports = (Boolean) group.getProperty(FakeMetadataObject.Props.UPDATE);
				break;
			default:
				throw new QueryMetadataException("Unknown group support constant: " + groupConstant); //$NON-NLS-1$
		}
		return supports.booleanValue();
	}

    public boolean elementSupports(Object elementID, int elementConstant)
        throws MetaMatrixComponentException,QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
    	FakeMetadataObject element = (FakeMetadataObject) elementID;
		Boolean supports = null;
		switch(elementConstant) {
			case SupportConstants.Element.NULL:
				supports = (Boolean) element.getProperty(FakeMetadataObject.Props.NULL);
				break;
            case SupportConstants.Element.NULL_UNKNOWN:
                supports = Boolean.FALSE;
                break;
			case SupportConstants.Element.SEARCHABLE_COMPARE:
				supports = (Boolean) element.getProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE);
				break;
			case SupportConstants.Element.SEARCHABLE_LIKE:
				supports = (Boolean) element.getProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE);
				break;
			case SupportConstants.Element.SELECT:
				supports = (Boolean) element.getProperty(FakeMetadataObject.Props.SELECT);
				break;
			case SupportConstants.Element.UPDATE:
				supports = (Boolean) element.getProperty(FakeMetadataObject.Props.UPDATE);
				break;
			case SupportConstants.Element.DEFAULT_VALUE:
                Object defaultValue = element.getProperty(FakeMetadataObject.Props.DEFAULT_VALUE);
                if(defaultValue == null) {
                    supports = Boolean.FALSE;
                } else if(defaultValue instanceof Boolean) {
                    supports = (Boolean) defaultValue;
                } else {
                    supports = Boolean.TRUE;
                }
				break;
			case SupportConstants.Element.AUTO_INCREMENT:
				supports = (Boolean) element.getProperty(FakeMetadataObject.Props.AUTO_INCREMENT);
				break;
            case SupportConstants.Element.CASE_SENSITIVE:
                supports = (Boolean) element.getProperty(FakeMetadataObject.Props.CASE_SENSITIVE);
                break;
            case SupportConstants.Element.SIGNED:
                supports = (Boolean) element.getProperty(FakeMetadataObject.Props.SIGNED);
                break;
			default:
				throw new QueryMetadataException("Unknown element support constant: " + elementConstant); //$NON-NLS-1$
		}
        if(supports != null) {
    		return supports.booleanValue();
        }
        return false;
	}

    public int getMaxSetSize(Object modelID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, modelID);
    	FakeMetadataObject model = (FakeMetadataObject) modelID;
        Integer maxSetSize = (Integer) model.getProperty(FakeMetadataObject.Props.MAX_SET_SIZE);
        if(maxSetSize == null) {
            return 100;
        }
        return maxSetSize.intValue();
    }

    public String getFullElementName(String fullGroupName, String shortElementName)
        throws MetaMatrixComponentException, QueryMetadataException {

        Assertion.isNotNull(fullGroupName);
        Assertion.isNotNull(shortElementName);

        return fullGroupName + "." + shortElementName; //$NON-NLS-1$
    }

    public String getShortElementName(String fullElementName)
        throws MetaMatrixComponentException, QueryMetadataException {

        Assertion.isNotNull(fullElementName);

        int index = fullElementName.lastIndexOf("."); //$NON-NLS-1$
        if(index >= 0) {
            return fullElementName.substring(index+1);
        }
        return fullElementName;
    }

    public String getGroupName(String fullElementName)
        throws MetaMatrixComponentException, QueryMetadataException {

        Assertion.isNotNull(fullElementName);

        int index = fullElementName.lastIndexOf("."); //$NON-NLS-1$
        if(index >= 0) {
            return fullElementName.substring(0, index);
        }
        return null;
    }


    public StoredProcedureInfo getStoredProcedureInfoForProcedure(String fullyQualifiedProcedureName)
        throws MetaMatrixComponentException, QueryMetadataException {

        Assertion.isNotNull(fullyQualifiedProcedureName);

        FakeMetadataObject procedureID = store.findObject(fullyQualifiedProcedureName, FakeMetadataObject.PROCEDURE);
        if(procedureID == null) {
            throw new QueryMetadataException("Unknown stored procedure: " + fullyQualifiedProcedureName); //$NON-NLS-1$
        }

        StoredProcedureInfo procInfo = new StoredProcedureInfo();
        procInfo.setProcedureID(procedureID);
        procInfo.setModelID(procedureID.getProperty(FakeMetadataObject.Props.MODEL));
        procInfo.setQueryPlan((QueryNode)procedureID.getProperty(FakeMetadataObject.Props.PLAN));
        procInfo.setProcedureCallableName((String)procedureID.getProperty(FakeMetadataObject.Props.CALLABLE_NAME));
        procInfo.setUpdateCount(((Integer)procedureID.getProperty(FakeMetadataObject.Props.UPDATE_COUNT, new Integer(-1))).intValue());

        // Read params
        List params = (List) procedureID.getProperty(FakeMetadataObject.Props.PARAMS);
        List paramInfos = new ArrayList(params.size());
        Iterator iter = params.iterator();
        while(iter.hasNext()) {
            FakeMetadataObject param = (FakeMetadataObject) iter.next();
            
            String name = param.getName();
            if(name.indexOf(".") < 0) { //$NON-NLS-1$
                name = procedureID.getName() + "." + name; //$NON-NLS-1$
            }            
            
            int index = ( (Integer) param.getProperty(FakeMetadataObject.Props.INDEX) ).intValue();
            int direction = ( (Integer) param.getProperty(FakeMetadataObject.Props.DIRECTION) ).intValue();
            String dataTypeName = (String) param.getProperty(FakeMetadataObject.Props.TYPE);
            Class dataTypeClass = DataTypeManager.getDataTypeClass(dataTypeName);
            
            SPParameter paramInfo = new SPParameter(index, direction, name);
            paramInfo.setParameterType(direction);
            paramInfo.setMetadataID(param);
            if(direction == ParameterInfo.RESULT_SET) {
                paramInfo.setClassType(java.sql.ResultSet.class);
            } else {
                paramInfo.setClassType(dataTypeClass);
            }
            
            FakeMetadataObject resultSet = (FakeMetadataObject)param.getProperty(FakeMetadataObject.Props.RESULT_SET);
            if(resultSet != null){
            	Iterator iter2 = ((List)resultSet.getProperty(FakeMetadataObject.Props.COLUMNS)).iterator();
            	while(iter2.hasNext()){
                    ElementSymbol col = (ElementSymbol) iter2.next();
            		paramInfo.addResultSetColumn(col.getName(), col.getType(), col.getMetadataID());
            	}
            }
            paramInfos.add(paramInfo);
        }

        procInfo.setParameters(paramInfos);

        return procInfo;
	}

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getIndexesInGroup(java.lang.Object)
     */
    public Collection getIndexesInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
        return getTypeOfKeysInGroup(groupID, FakeMetadataObject.TYPE_INDEX);
    }

    public Collection getUniqueKeysInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	return getTypeOfKeysInGroup(groupID, FakeMetadataObject.TYPE_PRIMARY_KEY);
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getForeignKeysInGroup(java.lang.Object)
     */
    public Collection getForeignKeysInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	return getTypeOfKeysInGroup(groupID, FakeMetadataObject.TYPE_FOREIGN_KEY);
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getPrimaryKeyIDForForeignKeyID(java.lang.Object)
     */
    public Object getPrimaryKeyIDForForeignKeyID(Object foreignKeyID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, foreignKeyID);
    	FakeMetadataObject keyObj = (FakeMetadataObject) foreignKeyID;
        return keyObj.getProperty(FakeMetadataObject.Props.REFERENCED_KEY);
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInIndex(java.lang.Object)
     */
    public List getElementIDsInIndex(Object index)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, index);
        FakeMetadataObject keyObj = (FakeMetadataObject) index;
        return (List)keyObj.getProperty(FakeMetadataObject.Props.KEY_ELEMENTS);
    }

    public List getElementIDsInKey(Object keyID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, keyID);
		FakeMetadataObject keyObj = (FakeMetadataObject) keyID;
		return (List)keyObj.getProperty(FakeMetadataObject.Props.KEY_ELEMENTS);
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getAccessPatternsInGroup(Object)
     */
    public Collection getAccessPatternsInGroup(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	return getTypeOfKeysInGroup(groupID, FakeMetadataObject.TYPE_ACCESS_PATTERN);
    }

	//Used to get either keys or access patterns
	private Collection getTypeOfKeysInGroup(Object groupID,
                                        final Integer KEY_TYPE) {
		ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
		FakeMetadataObject group = (FakeMetadataObject)groupID;

		Collection keys = (Collection)group.getProperty(FakeMetadataObject.Props.KEYS);
		if (keys == null){
			return Collections.EMPTY_LIST;
		}

		Collection keysOfType = new ArrayList(keys.size());
		Iterator keyIter = keys.iterator();
		while (keyIter.hasNext()) {
			FakeMetadataObject key = (FakeMetadataObject) keyIter.next();
			if (KEY_TYPE.equals(key.getProperty(FakeMetadataObject.Props.KEY_TYPE))){
				keysOfType.add(key);
			}
		}

		return keysOfType;
	}

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInAccessPattern(Object)
     */
    public List getElementIDsInAccessPattern(Object accessPattern)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, accessPattern);
    	FakeMetadataObject accessPatternObj = (FakeMetadataObject) accessPattern;
        return (List)accessPatternObj.getProperty(FakeMetadataObject.Props.KEY_ELEMENTS);
    }

    public MappingNode getMappingNode(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
    	FakeMetadataObject group = (FakeMetadataObject) groupID;
        MappingDocument doc = (MappingDocument)group.getProperty(FakeMetadataObject.Props.PLAN);
        doc.setName(getFullName(groupID));
        return doc;
    }

    public boolean isXMLGroup(Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
    	FakeMetadataObject group = (FakeMetadataObject) groupID;
        Object plan = group.getProperty(FakeMetadataObject.Props.PLAN);
        if(plan == null) {
        	return false;
        }
      	return (plan instanceof MappingNode);
    }

    public String getVirtualDatabaseName() throws MetaMatrixComponentException, QueryMetadataException {
        return "myvdb"; //$NON-NLS-1$
    }
    
    public boolean isTemporaryTable(Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
    	FakeMetadataObject group = (FakeMetadataObject) groupID;
        Boolean isTemp = (Boolean)group.getProperty(FakeMetadataObject.Props.TEMP);
        if(isTemp != null && isTemp.equals(Boolean.TRUE)) {
            return true;
        }
        return false;
    }    

	public Collection getXMLTempGroups(Object groupID)
        throws MetaMatrixComponentException, QueryMetadataException{

		ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
		MappingDocument mappingDoc = (MappingDocument)((FakeMetadataObject)groupID).getProperty(FakeMetadataObject.Props.PLAN);
        List tempGroups = resolveGroups(getStagingTables(mappingDoc));
        return tempGroups;
    }
    
    List getStagingTables(MappingDocument doc) {
        final List tables = new ArrayList();
        
        // visitor to extract all the explicit staging tables.
        MappingVisitor visitor = new MappingVisitor() {
            public void visit(MappingBaseNode baseNode) {
                if (baseNode.getStagingTables() != null) {
                    tables.addAll(baseNode.getStagingTables());
                }
            }
        };
        doc.acceptVisitor(new Navigator(true, visitor));
        return tables;
    }
    

    private List resolveGroups(List groupNames)
        throws QueryMetadataException, MetaMatrixComponentException {
        
        if(groupNames != null && !groupNames.isEmpty()) {
            ArrayList tempGroups = new ArrayList();
            for(int i = 0; i < groupNames.size(); i++) {           
                tempGroups.add(this.getGroupID((String)groupNames.get(i)));
            }
            return tempGroups;
        }
        return Collections.EMPTY_LIST;
    }

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getCardinality(java.lang.Object)
	 */
	public int getCardinality(Object groupID)
		throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
		Integer cardinality = (Integer)((FakeMetadataObject)groupID).getProperty(FakeMetadataObject.Props.CARDINALITY);
		if (cardinality != null){
			return cardinality.intValue();
		}
		return QueryMetadataInterface.UNKNOWN_CARDINALITY;
	}

    public String getNameInSource(Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, metadataID);
        return (String)((FakeMetadataObject)metadataID).getProperty(FakeMetadataObject.Props.NAME_IN_SOURCE);
    }

    public Properties getExtensionProperties(Object metadataID)
        throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, metadataID);
        return ((FakeMetadataObject)metadataID).getExtensionProps();
    }

    public int getElementLength(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element = (FakeMetadataObject) elementID;
	    return Integer.parseInt((String) element.getProperty(FakeMetadataObject.Props.LENGTH));
    }

    /**
     * Return position of element in group or result set.  Position returned is 1-based!
     */
    public int getPosition(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
	    FakeMetadataObject element = (FakeMetadataObject) elementID;
        return ((Integer) element.getProperty(FakeMetadataObject.Props.INDEX)).intValue() + 1;
    }

    public int getPrecision(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element = (FakeMetadataObject) elementID;
        String precision = (String) element.getProperty(FakeMetadataObject.Props.PRECISION);
        if (precision == null) {
            return 0;
        }
        return Integer.parseInt(precision);
    }

    public int getRadix(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element = (FakeMetadataObject) elementID;
        String radix = (String) element.getProperty(FakeMetadataObject.Props.RADIX);
        if (radix == null) {
            return 0;
        }
        return Integer.parseInt(radix);
    }

    public int getScale(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element = (FakeMetadataObject) elementID;
        String scale = (String) element.getProperty(FakeMetadataObject.Props.SCALE);
        if (scale == null) {
            return 0;
        }
        return Integer.parseInt(scale);
    }

    
    public String getNativeType(Object elementID) throws MetaMatrixComponentException,
                                                 QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element =  (FakeMetadataObject) elementID;
        String nativeType = (String) element.getProperty(FakeMetadataObject.Props.NATIVE_TYPE);
        if(nativeType == null) {
            return "";                 //$NON-NLS-1$
        }
        return nativeType;
    }
    
    public boolean hasMaterialization(Object groupID) throws MetaMatrixComponentException,
                                                     QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
        FakeMetadataObject group = (FakeMetadataObject)groupID;
        return group.getProperty(FakeMetadataObject.Props.MAT_GROUP) != null;
    }

    public Object getMaterialization(Object groupID) throws MetaMatrixComponentException,
                                                    QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
        FakeMetadataObject group =  (FakeMetadataObject) groupID;
        return group.getProperty(FakeMetadataObject.Props.MAT_GROUP);
    }
    
    public Object getMaterializationStage(Object groupID) throws MetaMatrixComponentException,
                                                         QueryMetadataException {
        
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
        FakeMetadataObject group =  (FakeMetadataObject) groupID;
        return group.getProperty(FakeMetadataObject.Props.MAT_STAGE);
    }
    
    public Object getMaximumValue(Object elementID) throws MetaMatrixComponentException,
                                                   QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element =  (FakeMetadataObject) elementID;
        return element.getProperty(FakeMetadataObject.Props.MAX_VALUE);
    }
    
    public Object getMinimumValue(Object elementID) throws MetaMatrixComponentException,
                                                   QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element =  (FakeMetadataObject) elementID;
        return element.getProperty(FakeMetadataObject.Props.MIN_VALUE);
    }

    public int getDistinctValues(Object elementID) throws MetaMatrixComponentException,
                                                   QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element = (FakeMetadataObject)elementID;
        Integer val = (Integer) element.getProperty(FakeMetadataObject.Props.DISTINCT_VALUES);
        if(val != null) {
            return val.intValue();
        }
        return -1;
    }

    public int getNullValues(Object elementID) throws MetaMatrixComponentException,
                                                   QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element = (FakeMetadataObject)elementID;
        Integer val = (Integer) element.getProperty(FakeMetadataObject.Props.NULL_VALUES);
        if(val != null) {
            return val.intValue();
        }
        return -1;
    }

    public List getXMLSchemas(Object groupID) throws MetaMatrixComponentException,
                                             QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, groupID);
        FakeMetadataObject group =  (FakeMetadataObject) groupID;
        return (List) group.getProperty(FakeMetadataObject.Props.XML_SCHEMAS);
    }
    
	public boolean isProcedure(Object elementID) {
		ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element =  (FakeMetadataObject) elementID;
        return FakeMetadataObject.PROCEDURE.equals(element.getType());
	}
    
    public byte[] getBinaryVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return "ResourceContents".getBytes(); //$NON-NLS-1$
    }

    public String getCharacterVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return "ResourceContents"; //$NON-NLS-1$
    }

    public String[] getVDBResourcePaths() throws MetaMatrixComponentException, QueryMetadataException {
        return new String[] {"my/resource/path"}; //$NON-NLS-1$
    }
    
    /** 
     * @see com.metamatrix.query.metadata.BasicQueryMetadata#getModeledType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledType(Object elementID) throws MetaMatrixComponentException,
                                                  QueryMetadataException {
        
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element =  (FakeMetadataObject) elementID;
        return (String) element.getProperty(FakeMetadataObject.Props.MODELED_TYPE);
    }
    
    /** 
     * @see com.metamatrix.query.metadata.BasicQueryMetadata#getModeledBaseType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledBaseType(Object elementID) throws MetaMatrixComponentException,
                                                      QueryMetadataException {
        
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
    	FakeMetadataObject element =  (FakeMetadataObject) elementID;
        return (String) element.getProperty(FakeMetadataObject.Props.MODELED_BASE_TYPE);
    }
    
    /** 
     * @see com.metamatrix.query.metadata.BasicQueryMetadata#getModeledPrimitiveType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledPrimitiveType(Object elementID) throws MetaMatrixComponentException,
                                                           QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, elementID);
        FakeMetadataObject element =  (FakeMetadataObject) elementID;
        return (String) element.getProperty(FakeMetadataObject.Props.MODELED_PRIMITIVE_TYPE);
    }
    
    @Override
    public Object addToMetadataCache(Object metadataID, String key, Object value)
    		throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, metadataID);
    	FakeMetadataObject object = (FakeMetadataObject) metadataID;
    	synchronized (object) {
        	Object result = object.getProperty(key);
        	object.putProperty(key, value);
        	return result;
		}
    }
    
    @Override
    public Object getFromMetadataCache(Object metadataID, String key)
    		throws MetaMatrixComponentException, QueryMetadataException {
    	ArgCheck.isInstanceOf(FakeMetadataObject.class, metadataID);
    	FakeMetadataObject object =  (FakeMetadataObject) metadataID;
    	synchronized (object) {
        	return object.getProperty(key);
		}
    }
}
