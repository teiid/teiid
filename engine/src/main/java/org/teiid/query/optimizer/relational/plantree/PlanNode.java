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

package org.teiid.query.optimizer.relational.plantree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


public class PlanNode {

    // --------------------- Node State --------------------------

    /** The type of node, as defined by NodeConstants.Types. */
    private int type;

    /** The parent of this node, null if root. */
    private PlanNode parent;

    /** Child nodes, usually just 1 or 2, but occasionally more */
    private LinkedList<PlanNode> children = new LinkedList<PlanNode>();
    
    private List<PlanNode> childrenView = Collections.unmodifiableList(children);
    
    /** Type-specific node properties, as defined in NodeConstants.Info. */
    private Map<NodeConstants.Info, Object> nodeProperties;

    // --------------------- Planning Info --------------------------

    /** The set of groups that this node deals with. */
    private Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        
    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    public PlanNode() {    
    }    
    
    // =========================================================================
    //                     A C C E S S O R      M E T H O D S
    // =========================================================================

    public int getType() {
        return type;
    }    

    public void setType(int type) {
        this.type = type;
    }    

    public PlanNode getParent() {
        return parent;
    }

    private void setParent(PlanNode parent) {
    	if (this.parent != null) {
    		this.parent.children.remove(this);
    	}
    	this.parent = parent;
    }

    public List<PlanNode> getChildren() {
        return this.childrenView;
    }
    
    public List<PlanNode> removeAllChildren() {
    	ArrayList<PlanNode> childrenCopy = new ArrayList<PlanNode>(children);
    	for (Iterator<PlanNode> childIter = this.children.iterator(); childIter.hasNext();) {
    		PlanNode child = childIter.next();
    		childIter.remove();
    		child.parent = null;
    	}
    	return childrenCopy;
    }
    
    public int getChildCount() {
        return this.children.size();
    }
        
    public PlanNode getFirstChild() {
        if ( getChildCount() > 0 ) {
            return this.children.getFirst();
        }
        return null;
    }
    
    public PlanNode getLastChild() {
        if ( getChildCount() > 0 ) {
            return this.children.getLast();
        }
        return null;
    }
        
    public void addFirstChild(PlanNode child) {
        this.children.addFirst(child);
        child.setParent(this);
    }
    
    public void addLastChild(PlanNode child) {
        this.children.addLast(child);
        child.setParent(this);
    }
    
    public void addChildren(Collection<PlanNode> otherChildren) {
        for (PlanNode planNode : otherChildren) {
			this.addLastChild(planNode);
		}
    }
    
    public PlanNode removeFromParent() {
    	PlanNode result = this.parent;
    	if (result != null) {
    		result.removeChild(this);
    	}
    	return result;
    }
    
    public boolean removeChild(PlanNode child) {
        boolean result = this.children.remove(child);
        if (result) {
        	child.parent = null;
        } 
        return result;
    }    
                        
    public Object getProperty(NodeConstants.Info propertyID) {
        if(nodeProperties == null) {
            return null;
        }
        return nodeProperties.get(propertyID);
    }

    public Object setProperty(NodeConstants.Info propertyID, Object value) {
        if(nodeProperties == null) {
            nodeProperties = new HashMap<NodeConstants.Info, Object>();
        }    
        return nodeProperties.put(propertyID, value);
    }

    public Object removeProperty(Object propertyID) {
        if(nodeProperties == null) {
            return null;
        }   
        return nodeProperties.remove(propertyID);
    }
    
    /**
     * Indicates if there is a non-null value for the property
     * key or not
     * @param propertyID one of the properties from {@link NodeConstants}
     * @return whether this node has a non-null value for that property
     */
    public boolean hasProperty(NodeConstants.Info propertyID) {
        return (getProperty(propertyID) != null);
    }

    /**
     * Indicates if there is a non-null and non-empty Collection value for the property
     * key or not
     * @param propertyID one of the properties from {@link NodeConstants} which is 
     * known to be a Collection object of some sort
     * @return whether this node has a non-null and non-empty Collection 
     * value for that property
     */
    public boolean hasCollectionProperty(NodeConstants.Info propertyID) {
        Collection<Object> value = (Collection<Object>)getProperty(propertyID);
        return (value != null && !value.isEmpty());
    }
    
    public void addGroup(GroupSymbol groupID) {
        groups.add(groupID);
    }

    public void addGroups(Collection<GroupSymbol> newGroups) {
        this.groups.addAll(newGroups);
    }
        
    public Set<GroupSymbol> getGroups() {
        return groups;
    }

    // =========================================================================
    //            O V E R R I D D E N    O B J E C T     M E T H O D S
    // =========================================================================

    /**
     * Print plantree structure starting at this node
     * @return String representing this node and all children under this node
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        getRecursiveString(str, 0);
        return str.toString();
    }

    /**
     * Just print single node to string instead of node+recursive plan.
     * @return String representing just this node
     */
    public String nodeToString() {
        StringBuffer str = new StringBuffer();
        getNodeString(str);
        return str.toString();
    }
    
    // Define a single tab
    private static final String TAB = "  "; //$NON-NLS-1$
    
    private void setTab(StringBuffer str, int tabStop) {
        for(int i=0; i<tabStop; i++) {
            str.append(TAB);
        }            
    }
    
    void getRecursiveString(StringBuffer str, int tabLevel) {
        setTab(str, tabLevel);
        getNodeString(str);
        str.append(")\n");  //$NON-NLS-1$
        
        // Recursively add children at one greater tab level
        for (PlanNode child : children) {
            child.getRecursiveString(str, tabLevel+1);
        }        
    }

    void getNodeString(StringBuffer str) {
        str.append(NodeConstants.getNodeTypeString(this.type));
        str.append("(groups="); //$NON-NLS-1$
        str.append(this.groups);
        if(nodeProperties != null) {
            str.append(", props="); //$NON-NLS-1$
            str.append(nodeProperties);
        }
    }
    
    public boolean hasBooleanProperty(NodeConstants.Info propertyKey) {
        return Boolean.TRUE.equals(getProperty(propertyKey));
    }
    
    public void replaceChild(PlanNode child, PlanNode replacement) {
    	int i = this.children.indexOf(child);
    	this.children.set(i, replacement);
    	child.setParent(null);
    	replacement.setParent(this);
    }
    
    /**
     * Add the node as this node's parent.  NOTE: This node 
     * must already have a parent.
     * @param node
     */
    public void addAsParent(PlanNode node) {
    	if (this.parent != null) {
        	this.parent.replaceChild(this, node);
    	}
    	assert node.getChildCount() == 0;
		node.addLastChild(this);
    }
    
    public List<SymbolMap> getCorrelatedReferences() {
    	List<SubqueryContainer> containers = getSubqueryContainers();
    	if (containers.isEmpty()) {
    		return Collections.emptyList();
    	}
    	ArrayList<SymbolMap> result = new ArrayList<SymbolMap>(containers.size());
    	for (SubqueryContainer container : containers) {
    		SymbolMap map = container.getCommand().getCorrelatedReferences();
			if (map != null) {
				result.add(map);
			}
		}
    	return result;
    }
    
    public List<SymbolMap> getAllReferences() {
    	List<SymbolMap> refMaps = new ArrayList<SymbolMap>(getCorrelatedReferences());
        refMaps.addAll(getExportedCorrelatedReferences());
        return refMaps;
    }
    
    public List<SymbolMap> getExportedCorrelatedReferences() {
    	if (type != NodeConstants.Types.JOIN) {
    		return Collections.emptyList();
    	}
    	LinkedList<SymbolMap> result = new LinkedList<SymbolMap>();
		for (PlanNode child : NodeEditor.findAllNodes(this, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS)) {
			SymbolMap references = (SymbolMap)child.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
	        if (references == null) {
	        	continue;
	        }
        	Set<GroupSymbol> correlationGroups = GroupsUsedByElementsVisitor.getGroups(references.getValues());
        	PlanNode joinNode = NodeEditor.findParent(child, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
        	while (joinNode != null) {
        		if (joinNode.getGroups().containsAll(correlationGroups)) {
        			if (joinNode == this) {
        				result.add(references);
        			}
        			break;
        		}
        		joinNode = NodeEditor.findParent(joinNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
        	}
		}
        return result;
    }
    
    public Set<ElementSymbol> getCorrelatedReferenceElements() {
        List<SymbolMap> maps = getCorrelatedReferences();
        
        if(maps.isEmpty()) {
            return Collections.emptySet();    
        }
        HashSet<ElementSymbol> result = new HashSet<ElementSymbol>();
        for (SymbolMap symbolMap : maps) {
	        List<Expression> values = symbolMap.getValues();
	        for (Expression expr : values) {
	            ElementCollectorVisitor.getElements(expr, result);
	        }
        }
        return result;
    }
    
	public List<SubqueryContainer> getSubqueryContainers() {
		Collection<? extends LanguageObject> toSearch = Collections.emptyList();
		switch (this.getType()) {
			case NodeConstants.Types.SELECT: {
				Criteria criteria = (Criteria) this.getProperty(NodeConstants.Info.SELECT_CRITERIA);
				toSearch = Arrays.asList(criteria);
				break;
			}
			case NodeConstants.Types.PROJECT: {
				toSearch = (Collection) this.getProperty(NodeConstants.Info.PROJECT_COLS);
				break;
			}
			case NodeConstants.Types.JOIN: {
				toSearch = (List<Criteria>) this.getProperty(NodeConstants.Info.JOIN_CRITERIA);
				break;
			}
		}
		return ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(toSearch);
	}
	
	public float getCardinality() {
		Float cardinality = (Float) this.getProperty(NodeConstants.Info.EST_CARDINALITY);
		if (cardinality == null) {
			return -1f;
		}
		return cardinality;
	}
        
}
