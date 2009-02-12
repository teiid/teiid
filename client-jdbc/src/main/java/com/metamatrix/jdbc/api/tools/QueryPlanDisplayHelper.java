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

package com.metamatrix.jdbc.api.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.metamatrix.jdbc.api.DisplayHelper;
import com.metamatrix.jdbc.api.PlanNode;

/**
 * 
 */
public class QueryPlanDisplayHelper implements DisplayHelper {
	
	//Maps containing display values by type			
	private Map nodeNameMap = new TreeMap();
    private Map childTypeMap = new TreeMap();
    private Map descriptionMap = new TreeMap();
    private Map propertyOrderMap = new TreeMap();
    private Map propertyNameMap = new TreeMap();
	
    /**
	 * Default constructor
	 */
	public QueryPlanDisplayHelper() {
        init();
	}
	
	/**
	 * 
	 */
	private void init() {
		/*	
		 * Node Name Mappings - to change node name for display
		 */	
	    nodeNameMap.put("Child Relational Plan", "Relational Plan"); //$NON-NLS-1$ //$NON-NLS-2$
        nodeNameMap.put("Child XML Plan", "XML Plan"); //$NON-NLS-1$ //$NON-NLS-2$
        nodeNameMap.put("COMMENT", "ADD COMMENT"); //$NON-NLS-1$ //$NON-NLS-2$
        
        childTypeMap.put("Relational Plan", "Child Relational Plan"); //$NON-NLS-1$ //$NON-NLS-2$
        childTypeMap.put("XML Plan", "Child XML Plan"); //$NON-NLS-1$ //$NON-NLS-2$
        		
		/*	
		 * Description Mapping logic
		 */
        descriptionMap.put("Join", "${joinType} ON ${joinCriteria}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("Project", "${selectCols}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("Select", "${criteria}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("Access", "${sql}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("EXECUTE SQL", "${sql}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("LOOP", "${joinType} ON ${joinCriteria}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("Join", "${joinType} ON ${joinCriteria}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("Limit", "${rowLimit}"); //$NON-NLS-1$ //$NON-NLS-2$
        descriptionMap.put("Offset", "${rowOffset}"); //$NON-NLS-1$ //$NON-NLS-2$
		
		/*	
		 * Property Order logic
		 */
        
        putPropertySortOrder("default", new String[] { "outputCols"}); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("Access", new String[] { "outputCols", "sql", "modelName"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("Group", new String[] { "outputCols", "groupCols"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("Join", new String[] { "outputCols", "joinType", "joinCriteria"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("Merge Join", new String[] { "outputCols", "joinType", "joinCriteria"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("Plan Execution", new String[] { "outputCols", "execPlan"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("Project Into", new String[] { "outputCols", "intoGrp", "selectCols"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("Project", new String[] { "outputCols", "selectCols"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("Dependent Project", new String[] { "outputCols", "selectCols"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("Select", new String[] { "outputCols", "criteria"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("Dependent Select", new String[] { "outputCols", "criteria"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("Sort", new String[] { "outputCols", "sortCols", "removeDups"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("Limit", new String[] { "outputCols", "rowLimit"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("Offset", new String[] { "outputCols", "rowOffset"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        putPropertySortOrder("ABORT", new String[] { "message" }); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("ADD COMMENT", new String[] { "message" }); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("ADD ELEMENT", new String[] { "tag", "optional", "dataCol", "namespace", "namespaceDeclarations", "default" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        putPropertySortOrder("ADD ATTRIBUTE", new String[] { "tag", "dataCol", "namespace", "default" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        putPropertySortOrder("CACHE", new String[] {  }); //$NON-NLS-1$
        putPropertySortOrder("UNCACHE", new String[] { }); //$NON-NLS-1$
        putPropertySortOrder("CLOSE RESULTSET", new String[] { "resultSet" }); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("END DOCUMENT", new String[] { }); //$NON-NLS-1$
        putPropertySortOrder("EXECUTE SQL", new String[] { "resultSet", "sql", "isStaging", "inMemory", "group", "program" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        putPropertySortOrder("CHOICE", new String[] { "conditions", "programs", "defaultProgram" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("START DOCUMENT", new String[] { "encoding", "formatted" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("NEXT ROW", new String[] { "resultSet" }); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("DOCUMENT UP", new String[] { }); //$NON-NLS-1$
        putPropertySortOrder("DOCUMENT DOWN", new String[] { }); //$NON-NLS-1$
        putPropertySortOrder("NO OP", new String[] { }); //$NON-NLS-1$
        putPropertySortOrder("ASSIGN REFERENCE", new String[] { "expression" }); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("LOOP", new String[] { "resultSet", "program" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        putPropertySortOrder("ASSIGNMENT", new String[] { "variable", "expression", "program" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("BREAK", new String[] { }); //$NON-NLS-1$
        putPropertySortOrder("CONTINUE", new String[] { }); //$NON-NLS-1$
        putPropertySortOrder("CREATE CURSOR", new String[] { "resultSet", "sql" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        putPropertySortOrder("DECLARE VARIABLE", new String[] { "variable"}); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("IF", new String[] { "criteria", "then", "else" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        putPropertySortOrder("RAISE ERROR", new String[] { "message" }); //$NON-NLS-1$ //$NON-NLS-2$
        putPropertySortOrder("WHILE", new String[] { "criteria", "program" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
		/*	
		 * Property Name logic
		 */
        propertyNameMap.put("outputCols", "Output Columns"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("criteria", "Criteria"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("selectCols", "Select Columns"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("groupCols", "Grouping Columns"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("sql", "Source Query"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("modelName", "Model Name"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("joinType", "Join Type"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("joinCriteria", "Join Criteria"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("joinStrategy", "Join Strategy"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("execPlan", "Execution Plan"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("intoGrp", "Select Into Group"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("sortCols", "Sort Columns"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("removeDups", "Remove Duplicates"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("message", "Message"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("tag", "XML Node Name"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("namespace", "Namespace"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("dataCol", "Data Column"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("namespaceDeclarations", "Namespace Declarations"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("optional", "Optional Flag"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("default", "Default Value"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("program", "Sub Program"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("recurseDir", "Recursion Direction"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("resultSet", "Result Set"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("bindings", "Bindings"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("isStaging", "Is Staging Flag"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("inMemory", "Source In Memory Flag"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("conditions", "Conditions"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("programs", "Sub Programs"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("defaultProgram", "Default Programs"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("encoding", "Encoding"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("formatted", "Formatting Flag"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("expression", "Expression"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("variable", "Variable"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("group", "Group"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("then", "Then"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("else", "Else"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("nodeStatistics", "Statistics"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("nodeCostEstimates", "Cost Estimates"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("rowLimit", "Row Limit"); //$NON-NLS-1$ //$NON-NLS-2$
        propertyNameMap.put("rowOffset", "Row Offset"); //$NON-NLS-1$ //$NON-NLS-2$
	}	
	
    /**
     * Put property sort order for a particular type in the workOrderMap 
     * @param type The node type
     * @param propOrder Sorted list of property names
     * @since 5.0
     */
    private void putPropertySortOrder(String type, String[] propOrder) {
        NodeProperty[] nodeProps = new NodeProperty[propOrder.length];
        for(int i=0; i<propOrder.length; i++) {
            nodeProps[i] = new NodeProperty(new Integer(i), type, propOrder[i]);            
        }
        propertyOrderMap.put(type, nodeProps);            
    }
    
	/**
	 * 
	 */
	public String getName(PlanNode node)
	{
		String name = (String) node.getProperties().get("type"); //$NON-NLS-1$
        
        /*
         * here we check to see if the type was null. If it was null, no need to look for a null key in the tree map.
         */
        if (name != null) {
            if(nodeNameMap.containsKey(name)) {
                name = (String)nodeNameMap.get(name);
            }
        } else {
            name = "Node"; //$NON-NLS-1$
        }
        
        name+=getDescription(node);
		
		return name;
	}
	
	/**
	 * 
	 */
	public String getDescription(PlanNode node)
	{
		Map nodeProps = node.getProperties();
        String description = ""; //$NON-NLS-1$

        // String nodeName="";
        String type = (String)nodeProps.get("type"); //$NON-NLS-1$
        if (type != null) {
            description = (String)nodeProps.get("desc"); //$NON-NLS-1$

            if (description == null || description.equals("")) //$NON-NLS-1$
            {
                description = (String)descriptionMap.get(type);
            }
        }
        
        if (description == null || description.trim().length() == 0) {
            description = ""; //$NON-NLS-1$
        } else {
            description = replaceProperties(description.trim(), nodeProps).trim();
            if (description.length() > 0 ) {
                description = " [" + description + "]"; //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
		
		return description;		
	}
    
    private String replaceProperties(String descriptionExpression, Map nodeProps) {
        int startIndex = descriptionExpression.indexOf('$');
        if (startIndex < 0) {
            return descriptionExpression;
        }
        int endIndex = descriptionExpression.indexOf('}');
        if (endIndex < 0) {
            return descriptionExpression;
        }
        String propertyName = descriptionExpression.substring(startIndex+2, endIndex).trim();
        Object descriptionObject = nodeProps.get(propertyName);
        String replacement = ""; //$NON-NLS-1$
        if (descriptionObject != null) {
            if (descriptionObject instanceof Collection) {
                replacement = stringifyCollection((Collection)descriptionObject);
            } else {
                replacement = descriptionObject.toString();
            }
        }
        descriptionExpression = descriptionExpression.substring(0, startIndex)
                                + replacement
                                + ((endIndex < descriptionExpression.length() - 1)
                                    ? descriptionExpression.substring(endIndex + 1) 
                                    : ""); //$NON-NLS-1$
        return replaceProperties(descriptionExpression, nodeProps);
    }
    
    private String stringifyCollection(Collection collection) {
        Iterator iterator = collection.iterator();
        if (iterator.hasNext()) {
            StringBuffer buffer = new StringBuffer(iterator.next().toString());
            while(iterator.hasNext()) {
                buffer.append(", ").append(iterator.next().toString()); //$NON-NLS-1$
            }
            return buffer.toString();
        }
        return ""; //$NON-NLS-1$
    }
	
	/**
	 * 
	 */
	public String getType(PlanNode node)
	{
		Map nodeProps  = node.getProperties();
        String type = (String) nodeProps.get("type"); //$NON-NLS-1$
		if (type==null)
		{
			type=""; //$NON-NLS-1$
		}
        if (node.getParent() != null) {
            if (childTypeMap.get(type) != null) {
                return (String)childTypeMap.get(type);
            }
        }
		return type;
	}
	
	/**
	 * 
	 */
	public Map getOrderedPropertiesMap(PlanNode node)
	{
		/* This methods returns a Map of Maps that represents the passed node's 
		 * properties in sorted order. The key for the main map is an Integer 
		 * value representing the sequence no. It's value is a Map that uses property 
		 * name/type for its key and the properties description for its value.		  
		 */
		 
		//Create key array for this node's properties
		Object[] nodeTypeKeySetArray=node.getProperties().keySet().toArray();
		
		//Set the map variable for this nope's properties.
		Map nodeProperties = node.getProperties();
		
		//Set the type variable for this node		
		String nodeType=(String)node.getProperties().get("type"); //$NON-NLS-1$
				
		//Create key array for the master properties ordered map
		Object[] orderedPropArray = propertyOrderMap.keySet().toArray();
		
		//Create an iterator for the master map
		Iterator orderedPropIterator = propertyOrderMap.values().iterator();
		
		Map propertiesOrdered=new TreeMap();
		//Map propertiesMap=new HashMap();
		NodeProperty[] nodeProps=null;
		
		int count = 0;

        if (nodeType != null) {
            // Loop through master map, find the current nodes ordered set, and add
            // to our Map of Maps.
            for (int i = 0; i < orderedPropArray.length; i++) {
                String orderedPropKey = (String)orderedPropArray[i];
                nodeProps = (NodeProperty[])orderedPropIterator.next();
                if (!(nodeType.equalsIgnoreCase(orderedPropKey))) {
                    continue;
                }// Else we found our nodes ordered set

                for (int j = 0; j < nodeProps.length; j++) {
                    String nodeProp = nodeProps[j].getValue();

                    if (nodeProperties.get(nodeProp) == null) {
                        // We don't have an instance of this property for
                        // this node so just go to the next one.
                    } else {
                        Map prop = new HashMap();
                        prop.put(nodeProp, nodeProperties.get(nodeProp));
                        propertiesOrdered.put(new Integer(j), prop);
                        count = j;

                    }
                }
            }

            if (propertiesOrdered.size() < node.getProperties().size()) {
                // We need to append missing properties to the ordered properties map.
                // This would happen when the node has properties that aren't listed
                // in the master ordered properties map.
                ArrayList addKeys = new ArrayList();
                for (int i = 0; i < node.getProperties().size(); i++) {
                    Iterator propIter = null;
                    propIter = propertiesOrdered.values().iterator();
                    boolean containsKey = false;
                    for (int k = 0; k < propertiesOrdered.size(); k++) {
                        Map propEntry = (Map)propIter.next();
                        if (propEntry.containsKey(nodeTypeKeySetArray[i])) {
                            //The property has already been added
                            containsKey = true;
                            break;
                        }
                    }
                    if (!containsKey) {
                        addKeys.add(nodeTypeKeySetArray[i]);
                    }
                }

                for (int i = 0; i < addKeys.size(); i++) {
                    count++;
                    Map prop = new HashMap();
                    prop.put(addKeys.get(i), node.getProperties().get(addKeys.get(i)));
                    propertiesOrdered.put(new Integer(count), prop);
                }
            }
        }
			
		return propertiesOrdered;
	}

    /**
     * This methods returns a List of sorted property names.        
     */    
	public List getOrderedProperties(PlanNode node)
	{
		Map orderPropMap = getOrderedPropertiesMap(node);
		Iterator mapIter = orderPropMap.keySet().iterator();
		List orderPropList=new ArrayList();
		
		while (mapIter.hasNext())
		{
            Object key = mapIter.next();
            Map valueMap = (Map) orderPropMap.get(key);
            String propName = (String) valueMap.keySet().iterator().next();
            if(!propName.equals("type") && !propName.equals("desc")) { //$NON-NLS-1$ //$NON-NLS-2$
                orderPropList.add(propName);
            }            
		}
		
		return orderPropList;		
	}

	/**
	 * 
	 */
	public void setMaxDescriptionLength(int maxLength)
	{
		//does nothing		
	}
	
	/**
	 * 
	 */
	public String getProperty(String property)
	{
		String propertyDisplay=(String)propertyNameMap.get(property);
		
		if (propertyDisplay==null)
		{
			propertyDisplay=property;
		}
		return propertyDisplay;		
	}
	
	/**
	 * 
	 */
	public String getPropertyName(String property)
	{
		String propertyDisplay=(String)propertyNameMap.get(property);
		
		if (propertyDisplay==null)
		{
			propertyDisplay=property;
		}
		return propertyDisplay;		
	}

	
	/*  Node property inner class used for storing properties
	 *  to sort for the PropertyOrderMap 
	 */
	private class NodeProperty
	{
		Integer seqNo = null;
		String name = ""; //$NON-NLS-1$
		String value = ""; //$NON-NLS-1$
		
		public NodeProperty(Integer seqNo,
							String name, 
							String value)
		{
			this.seqNo = seqNo;
			this.name = name;
			this.value = value;			
		}
		/**
		 * @return
		 */
		public String getName()
		{
			return name;
		}

		/**
		 * @return
		 */
		public Integer getSeqNo()
		{
			return seqNo;
		}

		/**
		 * @return
		 */
		public String getValue()
		{
			return value;
		}

		/**
		 * @param string
		 */
		public void setName(String string)
		{
			name = string;
		}

		/**
		 * @param integer
		 */
		public void setSeqNo(Integer integer)
		{
			seqNo = integer;
		}

		/**
		 * @param string
		 */
		public void setValue(String string)
		{
			value = string;
		}

	}
	
	/*  End of Node property inner class */
}
