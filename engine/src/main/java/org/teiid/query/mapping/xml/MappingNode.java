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

package org.teiid.query.mapping.xml;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.ElementSymbol;


/**
 * There will be a mapping node
 * for every entity (element or attribute) in a target XML document.
 * @see MappingNodeConstants
 */
public abstract class MappingNode implements Cloneable, Serializable {

	private static final long serialVersionUID = 6761829541871178451L;

	/** The parent of this node, null if root. */
    private MappingNode parent;

    /** Child nodes, usually just 1 or 2, but occasionally more */
    private List<MappingNode> children = new LinkedList<MappingNode>();

    /** node properties, as defined in NodeConstants.Properties. */
    private Map<MappingNodeConstants.Properties, Object> nodeProperties;

    /** default constructor */
    public MappingNode(){
    }

    /**
     * Get the parent of this node.
     */
    public MappingNode getParent() {
        return this.parent;
    }
    
    public static MappingNode findNode(MappingNode root, String partialName) {
        String canonicalName = root.getName();
        
        if (canonicalName != null) {
            //@ is optional, so we need to check for it only with attributes
            if (partialName.startsWith("@")) { //$NON-NLS-1$
                canonicalName = "@" + canonicalName; //$NON-NLS-1$
            }
            
            canonicalName = canonicalName.toUpperCase();
            
            boolean abort = true;
            
            if (partialName.startsWith(canonicalName)) {
                if (partialName.length() > canonicalName.length() + 1 && partialName.charAt(canonicalName.length()) == ElementSymbol.SEPARATOR.toCharArray()[0]) {
                    partialName = partialName.substring(canonicalName.length() + 1);
                    abort = false;
                } else if (partialName.length() == canonicalName.length()) {
                    return root;
                } 
            }
            
            if (abort) {
                return null;
            }
        }
        
        for (Iterator<MappingNode> i = root.getChildren().iterator(); i.hasNext();) {
            MappingNode child = i.next();
            MappingNode found = findNode(child, partialName);
            if (found != null) {
                return found;
            }
        }
        
        return null;
    }

    /**
     * Set the parent of this node.  This method is restricted, as
     * it should be called only when {@link #addChild adding a child node}
     */
    void setParent( MappingNode parent ) {
        this.parent = parent;
    }

    /**
     * Get the children contained by this node, or an empty List
     * @return children; if no children, return empty List (never null)
     */
    public List<MappingNode> getChildren(){
        return this.children;
    }

    /**
     * Get all children of this node of a specified target node type.  The value
     * of node type should be one of {@link #ATTRIBUTE} or {@link #ELEMENT}.
     */
    public List<MappingNode> getChildren( String type ) {
        List<MappingNode> subset = new ArrayList<MappingNode>();
        Iterator<MappingNode> iter = children.iterator();
        while ( iter.hasNext() ) {
            MappingNode node = iter.next();
            if ( node.getProperty(MappingNodeConstants.Properties.NODE_TYPE).equals(type) ) {
                subset.add( node );
            }
        }
        return subset;
    }
    
    public List<MappingNode> getNodeChildren() {
        List<MappingNode> subset = new ArrayList<MappingNode>();
        Iterator<MappingNode> iter = children.iterator();
        while ( iter.hasNext() ) {
            MappingNode node = iter.next();
            if ( !node.getProperty(MappingNodeConstants.Properties.NODE_TYPE).equals(MappingNodeConstants.ATTRIBUTE) ) {
                subset.add( node );
            }
        }
        return subset;
    }    

    /**
     * Add a child mapping node to this one.
     * @param MappingNode to add as a child of this node
     * @return the added node, with parent set to this node
     */
    public MappingNode addChild( MappingNode node ) {
        this.children.add( node );
        node.setParent(this);
        return node;
    }

    /**
     * Retrieve one of the Object values, keyed off the
     * Integer property keys defined in
     * {@link MappingNodeConstants.Properties}
     * @param propertyID Integer property key
     * @return Object value
     */
    public Object getProperty(MappingNodeConstants.Properties propertyID) {
        Object value = null;
        if(nodeProperties != null) {
            value = nodeProperties.get(propertyID);
        }
        if (value == null){
            value = MappingNodeConstants.Defaults.DEFAULT_VALUES.get(propertyID);
        }
        return value;
    }

    /**
     * Sets one of the Object values, keyed off the
     * Integer property keys defined in
     * {@link MappingNodeConstants.Properties}
     * @param propertyID Integer property key
     * @param value Object property value
     */
    void setProperty(MappingNodeConstants.Properties propertyID, Object value) {
        if (value != null){
            // Get the default for the property ...
            final Object defaultValue = MappingNodeConstants.Defaults.DEFAULT_VALUES.get(propertyID);
            final Map<MappingNodeConstants.Properties, Object> props = getNodeProperties();      // props is never null
            if ( !value.equals(defaultValue) ) {        // we know value is not null
                // Set the value only if different than the default; note that the 'getProperty'
                // method is returning the default if there isn't a value
                props.put(propertyID, value);
            } else {
                // The value is equal to the default, so because we didn't set it
                // we have to make sure to remove any existing value ...
                props.remove(propertyID);
            }
        }
    }

    void removeProperty(MappingNodeConstants.Properties propertyID) {
        getNodeProperties().remove(propertyID);
    }
    
    /**
     * Returns the actual local properties object, instantiates if necessary.
     * The Map returned is the basis for equality for a MappingNode.
     * <b>Note:</b> Use {@link #getProperties} unless absolutely necessary.
     * @return the actual properties (not including defaults) stored at this
     * object.
     * @see #getProperties
     */
    public Map<MappingNodeConstants.Properties, Object> getNodeProperties(){
        if(nodeProperties == null) {
            nodeProperties = new HashMap<MappingNodeConstants.Properties, Object>();
        }
        return nodeProperties;
    }


    /**
     * <p>Gets the fully qualified name of this node, this is obtained by concatenating
     * the nodes name to the parents qualified name with a delimiter in between them.</p>
     * @return The fully qualified name of this node.
     */
    public String getFullyQualifiedName() {
    	String myName = getPathName();
    	String parentName = (getParent() == null) ? "" : getParent().getFullyQualifiedName(); //$NON-NLS-1$

    	if(myName == null || myName.equals("")) { //$NON-NLS-1$
    		return parentName;
    	} else if(parentName == null || parentName.equals("")) { //$NON-NLS-1$
            return myName;
        } else {
    		return parentName + MappingNodeConstants.PATH_DELIM + myName;
    	}
    }
    
    public String getName() {
        return (String) this.getProperty(MappingNodeConstants.Properties.NAME);
    }
    
    public String getPathName() {
        return getName();
    }

    // =========================================================================
    // OVERRIDE Object METHODS
    // =========================================================================

    /**
     * Compare the symbol based ONLY on properties (including name), NOT on
     * parent node and children nodes (or lack thereof).
     * @param obj Other object
     * @return True if other obj is a MappingNode (or subclass) and properties
     * are equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(obj != null && obj instanceof MappingNode) {
            return ((MappingNode)obj).getNodeProperties().equals(getNodeProperties());
        }
        return false;
    }

    /**
     * Return a hash code for this symbol.
     * @return Hash code
     */
    public int hashCode() {
        return this.getNodeProperties().hashCode();
    }

    /**
     * Returns a string representation of an instance of this class.
     */
    public String toString() {
        Object name = getProperty(MappingNodeConstants.Properties.NAME);
        Object criteria = getProperty(MappingNodeConstants.Properties.CRITERIA);
        Object prefix = getProperty(MappingNodeConstants.Properties.NAMESPACE_PREFIX);
        Object defaultValue = getProperty(MappingNodeConstants.Properties.DEFAULT_VALUE);
        Object fixedValue = getProperty(MappingNodeConstants.Properties.FIXED_VALUE);
        Object namespaces = getProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS);

        return "[" + getProperty(MappingNodeConstants.Properties.NODE_TYPE) + "]" //$NON-NLS-1$ //$NON-NLS-2$
            + " name='" + ((prefix != null) ? prefix + ":" : "") + ((name != null) ? name : "")  + "'" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            //+ " path='" + ((name != null) ? getSchemaPath() : "undefined") + "'"
            +  ((defaultValue != null) ? " default='" + defaultValue + "'" : "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            +  ((fixedValue != null) ? " fixed='" + fixedValue + "'" : "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            + " minOccurs=" + getProperty(MappingNodeConstants.Properties.CARDINALITY_MIN_BOUND) //$NON-NLS-1$
            + " maxOccurs=" + getProperty(MappingNodeConstants.Properties.CARDINALITY_MAX_BOUND) //$NON-NLS-1$
            + ((criteria != null) ? (" constraint=\"" + criteria + "\"") : "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            + ((namespaces != null) ? (" namespaces=\"" + namespaces + "\"") : ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }


    /**
     * Prints the whole tree of MappingNodes to the provided PrintStream.
     */
    public static void printMappingNodeTree(MappingNode root, PrintStream output){
        output.print(toStringNodeTree(root));
    }

    public static String toStringNodeTree(MappingNode root){
        StringBuffer str = new StringBuffer();
        buildTreeString(root, str, 0);
        return str.toString();
    }

    // Define a single tab
    private static final String TAB = "  "; //$NON-NLS-1$

    private static void buildTreeString(MappingNode node, StringBuffer str, int tabLevel){
        setTab(str, tabLevel++);
        str.append(node.toString());
        str.append(node.getNodeProperties());
        str.append("\n"); //$NON-NLS-1$

        Iterator<MappingNode> i = node.getChildren().iterator();
        while (i.hasNext()){
            buildTreeString(i.next(), str, tabLevel);
        }
    }

    private static void setTab(StringBuffer str, int tabStop) {
        for(int i=0; i<tabStop; i++) {
            str.append(TAB);
        }
    }

    /**
     * Starting at a point in a mapping document and traversing either upward toward the root or
     * downward (depth or breadth first), this method finds and returns
     * the first MappingNode instance that has the indicated property key and value.  A <code>null</code>
     * can be passed in for the value parameter, indicating the first node should be returned which has
     * <i>any</i> non-null value for the property key.
     * @param propertyKey any of the properties defined in {@link MappingNodeConstants.Properties}
     * @param value an Object value that is checked for.  <code>null</code> can be passed in, indicating
     * the first node with any non-null value for that property should be returned
     * @param node a MappingNode in a mapping document - this is the beginning point of the search
     * @param searchDirection indicates whether to search upward or downward (depth or breadth first) from the
     * node parameter; use either {@link MappingNodeConstants#SEARCH_UP},
     * {@link MappingNodeConstants#SEARCH_DOWN} or {@link MappingNodeConstants#SEARCH_DOWN_BREADTH_FIRST}
     * @return MappingNode first node found that has the indicated property and value, or null if none found
     */
    static MappingNode findFirstNodeWithProperty(MappingNodeConstants.Properties propertyKey, Object value, MappingNode node, int searchDirection) {
        return findFirstNodeWithPropertyValue(propertyKey, value, false, node, searchDirection);
    }
    
    /**
     * Starting at a point in a mapping document and traversing either upward toward the root or
     * downward (depth or breadth first), this method finds and returns
     * the first MappingNode instance that has the indicated property key and value.  A <code>null</code>
     * can be passed in for the value parameter, indicating the first node should be returned which has
     * <i>any</i> non-null value for the property key.
     * @param propertyKey any of the properties defined in {@link MappingNodeConstants.Properties}
     * for which the value is of type String
     * @param value an Object value that is checked for.  <code>null</code> can be passed in, indicating
     * the first node with any non-null value for that property should be returned
     * @param node a MappingNode in a mapping document - this is the beginning point of the search
     * @param searchDirection indicates whether to search upward or downward (depth or breadth first) from the
     * node parameter; use either {@link MappingNodeConstants#SEARCH_UP},
     * {@link MappingNodeConstants#SEARCH_DOWN} or {@link MappingNodeConstants#SEARCH_DOWN_BREADTH_FIRST}
     * @return MappingNode first node found that has the indicated property and value, or null if none found
     */
    static MappingNode findFirstNodeWithPropertyString(MappingNodeConstants.Properties propertyKey, String value, MappingNode node, int searchDirection) {
        return findFirstNodeWithPropertyValue(propertyKey, value, true, node, searchDirection);
    }

    private static MappingNode findFirstNodeWithPropertyValue(MappingNodeConstants.Properties propertyKey, Object value, boolean isStringValue, MappingNode node, int searchDirection) {
        
        if (node == null || propertyKey == null){
            return null;
        }
        if (searchDirection == MappingNodeConstants.SEARCH_UP){
            return traverseUpForFirstNodeWithPropertyString(propertyKey, value, isStringValue, node);
        } else if (searchDirection == MappingNodeConstants.SEARCH_DOWN){
            return traverseDownForFirstNodeWithPropertyString(propertyKey, value, isStringValue, node, false);
        } else if (searchDirection == MappingNodeConstants.SEARCH_DOWN_BREADTH_FIRST){
            // Check root node first
            if (checkThisNodeForPropertyValue(propertyKey, value, isStringValue, node)){
                return node;
            }
            return traverseDownForFirstNodeWithPropertyString(propertyKey, value, isStringValue, node, true);
        } else {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.002.0009", searchDirection )); //$NON-NLS-1$
        }
    }

    private static MappingNode traverseDownForFirstNodeWithPropertyString(MappingNodeConstants.Properties propertyKey, Object value, boolean isStringValue, MappingNode node, boolean breadthFirst) {
        if (breadthFirst) {
            Iterator<MappingNode> children = node.getChildren().iterator();
            while (children.hasNext()){
                MappingNode child = children.next();
                if (checkThisNodeForPropertyValue(propertyKey, value, isStringValue, child)){
                    return child;
                }
            }
        } else {
            if (checkThisNodeForPropertyValue(propertyKey, value, isStringValue, node)){
                return node;
            }
        }
        
        Iterator<MappingNode> children = node.getChildren().iterator();
        while (children.hasNext()){
            MappingNode child = children.next();
            
            //recursive call to this method
            MappingNode result = traverseDownForFirstNodeWithPropertyString(propertyKey, value, isStringValue, child, breadthFirst);
            if (result != null){
                return result;
            }
        }

        return null;
    }
    
    private static boolean checkThisNodeForPropertyValue(MappingNodeConstants.Properties propertyKey, Object value, boolean isStringValue, MappingNode node) {
        Object thisValue = node.getProperty(propertyKey);
        if (thisValue != null){
            if (value == null){
                return true;
            }
            if (isStringValue &&  ((String)thisValue).equalsIgnoreCase((String)value)) {
                return true;
            }
            if (thisValue.equals(value)) {
                return true;
            }
        }
        return false;
    }

    private static MappingNode traverseUpForFirstNodeWithPropertyString(MappingNodeConstants.Properties propertyKey, Object value, boolean isStringValue, MappingNode node) {
        while (node != null){
            if (checkThisNodeForPropertyValue(propertyKey, value, isStringValue, node)){
                return node;
            }
            node = node.getParent();
        }
        return null;
    }
    
    public MappingNode setExclude(boolean exclude) {
        setProperty(MappingNodeConstants.Properties.IS_EXCLUDED, Boolean.valueOf(exclude));
        return this;
    }

    public boolean isExcluded() {
        Boolean exclude = (Boolean)getProperty(MappingNodeConstants.Properties.IS_EXCLUDED);
        if (exclude != null) {
            return exclude.booleanValue();
        }
        return false;
    }

    public abstract void acceptVisitor(MappingVisitor visitor);
    
    /**
     * Get the source node for this Mapping Node; note that only mapping elements and 
     * mapping attributes have the source nodes; that too ones with specified with
     * NameInSource specified attribute.
     * 
     * If not find nearest one looking up the tree. 
     * @return
     */
    public MappingSourceNode getSourceNode() {
        if (getParent() != null) {
            return getParent().getSourceNode();
        }
        return null;
    }

    public String getNameInSource() {
        return null;        
    }    
    
    public MappingNode clone() {
    	try {
			MappingNode clone = (MappingNode) super.clone();
			clone.children = new ArrayList<MappingNode>(children);
			for (int i = 0; i < clone.children.size(); i++) {
				clone.children.set(i, clone.children.get(i).clone());
				clone.children.get(i).setParent(clone);
			}
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new TeiidRuntimeException(e);
		}
    }
    
    public ElementSymbol getElementSymbol() {
    	return null;
    }
}
