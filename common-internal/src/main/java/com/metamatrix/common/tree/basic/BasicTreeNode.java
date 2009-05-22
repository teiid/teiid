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

package com.metamatrix.common.tree.basic;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.*;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.util.Assertion;

public class BasicTreeNode implements TreeNode, Serializable {

    public static final char DELIMITER_CHAR = '.';
    public static final String DELIMITER = "."; //$NON-NLS-1$

    private ObjectID globalUID;
    private ObjectDefinition type;
    private boolean exists = true;
    private String name;
    private List children;                  // list of TreeNode refs
    private List unmodifiableChildren;
    private BasicTreeNode parent;
    private boolean marked;
    private boolean modified;

    /**
     * The holder for the property values for this entity, keyed upon the
     * reference to the PropertyDefinition instance.
     * @link aggregationByValue
     * @supplierCardinality 0..1
     */
    private Map properties;
//    private Map unmodifiableProperties;

    /**
     * Construct a new instance by specifying the parent that owns this entity,
     * the name of the instance, the type of the instance and the global UID
     * for the instance.
     * <b>Note:</b> this constructor does <i>NOT</i> verify that the name is valid within
     * the parent's namespace.
     * @param parent the parent that is considered the owning namespace for this
     * instance; may be null if the new instance is to be the root
     * @param name the name for the new entity instance; may be null or zero length
     * if the node name is to be determined
     * @param type the reference to the MetaModelEntity that defines the type
     * of entity; may not be null
     * @param guid the globally-unique identifier for this instance; may not be null
     */
    public BasicTreeNode(BasicTreeNode parent, String name, ObjectDefinition defn, ObjectID guid ) {
//        Assertion.isNotNull(name,"The name may not be null");
        Assertion.isNotNull(defn,"The ObjectDefinition reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(guid,"The ObjectID reference may not be null"); //$NON-NLS-1$
        this.type = defn;
	    this.globalUID = guid;
        this.children = new ArrayList(5);
        this.unmodifiableChildren = Collections.unmodifiableList(this.children);
        this.properties = new HashMap();
//        this.unmodifiableProperties = Collections.unmodifiableMap(this.properties);
        this.setNameOfNode(name);
        this.modified = true;       // by default, a new object is considered modified

        if ( parent != null ) {
            parent.addChild(this,defn,-1);    // this call may not be efficient if 'exists' is false
            this.setExists(true);
        } else {
            this.ensureNameIsValid();
            this.setExists(true);  // this must be a root entity that should be made existent
        }
    }

	// ########################## PropertiedObject Methods ###################################

	// ########################## TreeNode Methods ###################################

    /**
     * Get this name of this entity.  The name does not contain the namespace,
     * but is unique within the namespace of the entity.
     * @return the entity's name; never null or zero-length
     */
    public String getName(){
	    return this.name;
    }

    /**
     * Get the actual or programmatic name of this property.
     * @return the property's name (never null)
     */
    public String getFullName(){
        return this.addFullName( new StringBuffer(), DELIMITER ).toString();
    }
    /**
     * Get the actual or programmatic name of this property.
     * @param delimiter the string delimiter to use between the names of nodes
     * @return the property's name (never null)
     */
    public String getFullName(String delimiter){
        return this.addFullName( new StringBuffer(), delimiter ).toString();
    }
    /**
     * Helper method to build a string of the full name without having to
     * construct a new StringBuffer in each node.
     * <p>
     * This method is implemented by having the node invoke this method on
     * its parent (so that the parent can add its full name first), and then
     * this node adds the delimiter and its name.
     * @param sb the StringBuffer to which the name of this node should be appended
     * @return the StringBuffer containing the full name of this node
     */
    protected StringBuffer addFullName(StringBuffer sb, String delimiter) {
        if ( delimiter == null || delimiter.length() == 0 ) {
            delimiter = DELIMITER;
        }
        StringBuffer result = sb;
        if ( parent != null ) {
            result = parent.addFullName(result,delimiter);
            result.append(delimiter);
        }
        result.append(this.name);
        return result;
    }
    /**
     * Get the actual or programmatic name of this property.
     * @return the property's name (never null)
     */
    public String getNamespace() {
        if ( this.parent == null ) {
            return ""; //$NON-NLS-1$
        }
        return this.parent.getFullName();
    }

    /**
     * Get this type for this entity.
     * @return the entity's type; never null
     */
    public ObjectDefinition getType() {
        return this.type;
    }

    /**
     * Return whether this TreeNode represents an existing metadata entity.
     * <p>
     * This attribute is set to true any time the object is part of a model,
     * and it is set to false whenever the object is removed (deleted) from
     * a model.
     * @return true the entry exists, or false otherwise.
     */
    public boolean exists() {
        return exists;
    }

    /**
     * Ensure that the existance flag on this instance is set to the specified
     * value.  If the values are the same, this method simply returns without
     * doing anything.  However, if the existance value is different that the
     * desired value, this method sets the existance value on this node and all
     * of its children (recursively).
     */
    public void setExists( boolean value ) {
        if ( this.exists != value ) {
            this.exists = value;
            Iterator iter = this.iterator();
            while ( iter.hasNext() ) {
                BasicTreeNode child = (BasicTreeNode) iter.next();
                child.setExists(value);
            }
        }
    }

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the abstract path.
     */
    public char getSeparatorChar() {
        return DELIMITER_CHAR;
    }

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    public String getSeparator() {
        return DELIMITER;
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof BasicTreeNode ) {
            BasicTreeNode that = (BasicTreeNode) obj;
            return ( this.globalUID.equals( that.getGlobalUID() ) );
		}

        // Otherwise not comparable ...
        return false;
    }
    /**
     * Compares this object to another. If the specified object is not an instance of
     * the ModelEntity class, then this method throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).
     * Note:  this method <i>is</i> consistent with <code>equals()</code>, meaning
     * that <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to; may not be null.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        BasicTreeNode that = (BasicTreeNode) obj;     // May throw ClassCastException
        Assertion.isNotNull(obj,"Attempt to compare null"); //$NON-NLS-1$
        if ( obj == this ) {
            return 0;
        }

        return this.globalUID.compareTo(that.getGlobalUID());
    }
    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString(){
        return this.getName();
    }

	// ########################## Implementation Methods ###################################

    public ObjectID getGlobalUID(){
        return globalUID;
    }

    /**
     * Use this with extreme care!!!!
     */
    protected void setGlobalUID(ObjectID newID){
        if ( newID != null ) {
            globalUID = newID;
        }
    }

    public Iterator iterator() {
        return this.unmodifiableChildren.iterator();
    }

    /**
     * Return the immediate child of this object that has the specified name.
     * @param name the name of the child to find; may not be null or zero-length
     * @param ignoreCase flag specifying true if the names should be compared in
     * a case-insensitive manner, or false if the names should compare in a
     * case-sensitive manner.
     * @return the list of children with the matching name, or null if no child is found.
     */
    public List getChildren( String name, boolean ignoreCase ) {
        Assertion.isNotNull(name,"The name reference may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(name,"The name reference may not be zero-length"); //$NON-NLS-1$
        List result = new ArrayList();
        Iterator iter = this.children.iterator();

        // Break into one of two loops based upon 'ignoreCase'; this is
        // done like this for runtime efficiency with the knowlege
        // that there is duplicate code.
        if ( ignoreCase ) {
            while ( iter.hasNext() ) {
                TreeNode obj = (TreeNode) iter.next();
                if ( obj.getName().equalsIgnoreCase(name) ) {
                    result.add(obj);
                }
            }
        } else {
            while ( iter.hasNext() ) {
                TreeNode obj = (TreeNode) iter.next();
                if ( obj.getName().equals(name) ) {
                    result.add(obj);
                }
            }
        }
        return result;
    }

    /**
     * Return the immediate child of this object that has the specified name.
     * @param name the name of the child to find; may not be null or zero-length
     * @param ignoreCase flag specifying true if the names should be compared in
     * a case-insensitive manner, or false if the names should compare in a
     * case-sensitive manner.
     * @return the child with the matching name, or null if no child is found.
     */
    public TreeNode getChild( String name, ObjectDefinition defn, boolean ignoreCase ) {
        Assertion.isNotNull(name,"The name reference may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(name,"The name reference may not be zero-length"); //$NON-NLS-1$
        Assertion.isNotNull(defn,"The ObjectDefinition reference may not be null"); //$NON-NLS-1$
        Iterator iter = this.children.iterator();

        // Break into one of two loops based upon 'ignoreCase'; this is
        // done like this for runtime efficiency with the knowlege
        // that there is duplicate code.
        if ( ignoreCase ) {
            while ( iter.hasNext() ) {
                TreeNode obj = (TreeNode) iter.next();
                if ( defn.equals(obj.getType()) && obj.getName().equalsIgnoreCase(name) ) {
                    return obj;
                }
            }
        } else {
            while ( iter.hasNext() ) {
                TreeNode obj = (TreeNode) iter.next();
                if ( defn.equals(obj.getType()) && obj.getName().equals(name) ) {
                    return obj;
                }
            }
        }
        return null;
    }

    public int getIndexOfChild( TreeNode child ) {
        TreeNode nodesParent = assertBasicTreeNode(child).parent;
        Assertion.assertTrue(this==nodesParent,"The referenced child is not contained in this entity"); //$NON-NLS-1$
        Iterator iter = this.children.iterator();
        int index = 0;
        while ( iter.hasNext() ) {
            if ( iter.next() == child ) {
                return index;
            }
            ++index;
        }
        throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0022));
    }

    /**
     * Return the ordered list of children.
     */
    public List getChildren() {
        return this.unmodifiableChildren;
    }

    /**
     * Return true if this node is allowed to have any children.
     */
//    public boolean getAllowsChildren() {
//        return CHILD_RULES.getAllowsChildren(this.getType());
//    }

    /**
     * Return true if this node currently does have any children.
     */
    public boolean hasChildren() {
        return ( this.unmodifiableChildren.size() != 0 );
    }

    public int getChildCount() {
        return this.unmodifiableChildren.size();
    }

    public boolean containsChildWithName( String name ) {
        Assertion.isNotNull(name,"The name reference may not be null"); //$NON-NLS-1$
        if ( name.length() == 0 ) {
            return false;
        }
        Iterator iter = this.iterator();
        while ( iter.hasNext() ) {
            BasicTreeNode child = (BasicTreeNode) iter.next();
            if ( child.getName().equals(name) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Set the name of this node.
     * <b>Note:</b> this method does <i>NOT</i> verify that the child's name is valid within this
     * namespace
     */
    public void setName(String newName) {
        this.setNameOfNode(newName);
        this.ensureNameIsValid();
    }

    protected void setNameOfNode(String newName) {
        this.name = newName;
        PropertyDefinition nameDefn = this.getNamePropertyDefinition();
        if ( nameDefn != null ) {
            this.setPropertyValue(nameDefn,newName);
        }
    }

    protected BasicTreeNode assertBasicTreeNode( TreeNode obj ) {
        Assertion.isNotNull(obj,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(obj instanceof BasicTreeNode,"The referenced object is not an BasicTreeNode"); //$NON-NLS-1$
        return (BasicTreeNode) obj;
    }

    /**
     * Get the model entity which which is the parent of this entity.
     * @return the parent model entity, which may be null if no parent exists.
     */
    public BasicTreeNode getParent() {
	    return this.parent;
    }

    /**
     * Determine whether this handle is the parent of the specified handle.
     * The result of this method is equivalent to calling the following,
     * although this method uses a much more efficient algorithm that
     * does not rely upon string comparisons:
     * <p>
     * <code>return this.getFullName().equals( node.getNamespace() )</code>
     * </p>
     * @return true if this handle is the namespace of the specified node
     */
    public boolean isParentOf(TreeNode node) {
        TreeNode nodesParent = assertBasicTreeNode(node).parent;
        return ( this == nodesParent );
    }

    /**
     * Determine whether this handle is an ancestor of the specified handle,
     * The result of this method is equivalent to calling the following,
     * although this method uses a much more efficient algorithm that
     * does not rely upon string comparisons:
     * <p>
     * <code>return node.getNamespace().startsWith( this.getFullName() )</code>
     * </p>
     * @return true if this handle is an ancestor of the specified node
     */
    public boolean isAncestorOf(TreeNode node) {
        TreeNode ancestor = assertBasicTreeNode(node).parent;
        while( ancestor != null ) {
            if ( this == ancestor ) {
                return true;
            }
            ancestor = assertBasicTreeNode(ancestor).parent;
        }
        return false;
    }

    /**
     * Return whether this node has undergone changes.  The time from which changes are
     * maintained is left to the implementation.
     * @return true if this TreeNode has changes, or false otherwise.
     */
    public boolean isModified() {
        return this.modified;
    }

    /**
     * Set the modified state of the TreeNode node.<br></br>Default behavior is to
     * set as modified the whole subtree starting with this node.
     * @param marked the marked state of the node.
     */
    public void setModified(boolean modified) {
        this.setModified(modified, true);
    }

    /**
     * Set the modified state of the TreeNode node.
     * @param modified the marked state of the node.
     * @param recursive true if the whole subtree starting from this node should
     * be set as modified, or false if only this node is to be set as modified.
     */
    public synchronized void setModified(boolean modified, boolean recursive) {
        this.modified = modified;
        if ( recursive ) {
            Iterator iter = this.children.iterator();
            while ( iter.hasNext() ) {
                BasicTreeNode child = (BasicTreeNode) iter.next();
                child.setModified(modified, true);
            }
        }
    }

    /**
     * Return the marked state of the specified node.
     * @return the marked state of the node.
     */
    public boolean isMarked() {
        return this.marked;
    }

    /**
     * Set the marked state of the TreeNode node.<br></br>Default behavior is to
     * mark the whole subtree from this node.
     * @param marked the marked state of the node.
     */
    public void setMarked(boolean marked) {
        this.setMarked(marked, true);
    }

    /**
     * Set the marked state of the TreeNode node.
     * @param marked the marked state of the node.
     * @param recursive mark the whole subtree starting from this node.
     */
    public synchronized void setMarked(boolean marked, boolean recursive) {
        this.marked = marked;
        if ( recursive ) {
            Iterator iter = this.children.iterator();
            while ( iter.hasNext() ) {
                BasicTreeNode child = (BasicTreeNode) iter.next();
                child.setMarked(marked, true);
            }
        }
    }

    /**
     * Return the set of marked nodes for this view.
     * @param the unmodifiable set of marked nodes; never null
     */
    protected synchronized void addMarkedNodesToSet( Set result ) {
        if ( this.isMarked() ) {
            result.add(this);
        }

        BasicTreeNode child = null;
        Iterator iter = this.children.iterator();
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            child.addMarkedNodesToSet( result );
        }
    }

    /**
     * Go through the tree and find out the leaf nodes beneath this object.
     * @return true if there are nodes below this node that are marked
     */
    public synchronized void findLeafNodes( Set leafNodes ) {
        if ( this.getChildCount() == 0 ) {
            leafNodes.add(this);
            return;
        }

        // Otherwise, call this same method on all children ...
        BasicTreeNode child = null;
        Iterator iter = this.children.iterator();
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            child.findLeafNodes( leafNodes );
        }
    }

    /**
     * Go through the tree and find out the top-level objects that are not marked
     * and that do not have marked nodes beneath them, and find the leaf-level marked
     * objects.
     * @return true if there are nodes below this node that are marked
     */
    public synchronized boolean findMarkedAndUnmarkedNodes( Set unmarkedNodes, Set markedLeafNodes ) {
        // If this node is marked, go no further ...
        if ( this.isMarked() ) {
            this.findLeafNodes(markedLeafNodes);
            return true;
        }
        unmarkedNodes.add(this);

        // Otherwise, call this same method on all children ...
        boolean childHasMarkedNodes = false;
        BasicTreeNode child = null;
        Iterator iter = this.children.iterator();
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            if ( child.findMarkedAndUnmarkedNodes( unmarkedNodes, markedLeafNodes ) ) {
                childHasMarkedNodes = true;
            }
        }

        if ( childHasMarkedNodes ) {
            unmarkedNodes.remove(this);
        }
        return childHasMarkedNodes;
    }

    /**
     * Go through the tree and find out the top-level objects that are not marked
     * and that do not have marked nodes beneath them, and find the leaf-level marked
     * objects.
     * @return true if there are nodes below this node that are marked
     */
    public synchronized void findTopLevelMarkedNodes( Set markedTopLevelNodes ) {
        // If this node is marked, go no further ...
        if ( this.isMarked() ) {
            markedTopLevelNodes.add(this);
        }

        // Otherwise, call this same method on all children ...
        BasicTreeNode child = null;
        Iterator iter = this.children.iterator();
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            child.findTopLevelMarkedNodes(markedTopLevelNodes);
        }
    }

    public Collection getDecendant( String pathWithName, String delimiter, boolean ignoreCase ) {
        if ( delimiter == null || delimiter.length() == 0 ) {
            delimiter = DELIMITER;
        }
        Set parents = new HashSet();
        Set decendents = new HashSet();
        parents.add(this);
        BasicTreeNode parent = this;
        StringTokenizer tokenizer = new StringTokenizer(pathWithName,delimiter);
        while ( tokenizer.hasMoreTokens() ) {
            String name = tokenizer.nextToken();
            decendents.clear();
            Iterator iter = parents.iterator();
            while ( iter.hasNext() ) {
                parent = (BasicTreeNode) iter.next();
                decendents.addAll( parent.getChildren( name, ignoreCase ) );
            }
            parents.clear();
            parents.addAll(decendents);
        }
        return decendents;
    }

    public Collection getDecendant( String pathWithName, boolean ignoreCase ) {
        return getDecendant(pathWithName,DELIMITER,ignoreCase);
    }

    /**
     * Remove from this node the specified child, null the parent reference
     * of the node, and mark the node as non-existant.
     * @param child the child entity
     */
    public synchronized void remove( BasicTreeNode child ) {
        Assertion.assertTrue(child!=this,"The specified node may not be removed from itself"); //$NON-NLS-1$
        Assertion.isNotNull(child,"Unable to remove a null child reference"); //$NON-NLS-1$
        boolean found = this.children.remove(child);
        Assertion.assertTrue(found,"The specified child was not found within this node"); //$NON-NLS-1$
        child.parent = null;
        child.setExists(false);
    }

    /**
     * Remove from this node the child specified by the index, null the parent reference
     * of the node, and mark the node as non-existant.
     * @param index the index of the child to be removed
     * @throws IndexOutOfBoundsException if the index is not within the range
     * <code>0 <= newIndex < this.getChildCount()</code>
     */
    public synchronized void removeAll() {
        Iterator iter = this.children.iterator();
        BasicTreeNode child = null;
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            child.parent = null;
            child.setExists(false);
            iter.remove();
        }
    }

    /**
     * Remove from this node the child specified by the index, null the parent reference
     * of the node, and mark the node as non-existant.
     * @param index the index of the child to be removed
     * @throws IndexOutOfBoundsException if the index is not within the range
     * <code>0 <= newIndex < this.getChildCount()</code>
     */
    public synchronized void remove(int index ) {
        BasicTreeNode child = (BasicTreeNode) this.children.remove(index);
        child.parent = null;
        child.setExists(false);
    }

    /**
     * Move the child to a new index under the same parent.
     * @param child the child entity
     * @param newIndex the location
     * @throws IndexOutOfBoundsException if the index is not within the range
     * <code>0 <= newIndex < this.getChildCount()</code>
     */
    public synchronized void moveChild( BasicTreeNode child, int newIndex ) {
        //this.remove(child);       // this marks it as non-existant, instead to ...
        Assertion.assertTrue(child!=this,"The specified node may not be removed from itself"); //$NON-NLS-1$
        Assertion.isNotNull(child,"Unable to remove a null child reference"); //$NON-NLS-1$
        int currentIndex = this.children.indexOf(child);
        if ( currentIndex == newIndex ) {
            return;
        }
        boolean found = this.children.remove(child);
        Assertion.assertTrue(found,"The specified child was not found within this node"); //$NON-NLS-1$
        int correctIndex = newIndex;
        if ( currentIndex < newIndex ) {
            // Change the location, because they computed the newIndex based upon the
            // list WITH the child, and we just removed the child (essentially decrementing
            // their index

            --correctIndex;
        }
        this.children.add(correctIndex,child);
    }

    /**
     * Insert the node as a child of this node.  If the child already has a parent,
     * this method removes the child from that parent.
     * <b>Note:</b> this method does <i>NOT</i> verify that the child's name is valid within this
     * namespace
     * @param child the child entity
     * @param newIndex the location
     * @throws IndexOutOfBoundsException if the index is not within the range
     * <code>0 <= newIndex <= this.getChildCount()</code>
     */
    public synchronized void insert( BasicTreeNode child, int index ) {
        Assertion.isNotNull(child,"The child reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(child != this,"The specified node may not be added to itself"); //$NON-NLS-1$
        Assertion.assertTrue(child.getParent() != this,"The specified node is already a child of this entity"); //$NON-NLS-1$

        this.children.add(index,child);             // ensures index is valid, so do this first
        if ( child.parent != null ) {
            child.parent.children.remove(child);    // doesn't mark node as non-existant
        }
        child.parent = this;                        // remove from parent
        child.setExists(true);                      // ensure that child is marked as exists

        // Check if this node already contains a child that has the same
        // name and type as the new child; if there is an existing child,
        // come up with the next valid name ...
        this.ensureNameIsValid();
    }

    /**
     * Insert the node as a child of this node, and place it at the end of this
     * node's list of children.  If the child already has a parent,
     * this method removes the child from that parent.
     * <p>
     * <b>Note:</b> this method does <i>NOT</i> verify that the child's name is valid within this
     * namespace
     * @param child the child entity
     */
    public synchronized void add( BasicTreeNode child ) {
        Assertion.isNotNull(child,"The child reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(child != this,"The specified node may not be added to itself"); //$NON-NLS-1$
        Assertion.assertTrue(child.getParent() != this,"The specified node is already a child of this entity"); //$NON-NLS-1$
        addChild(child,child.getType(),-1);     // add at end
    }

    /**
     * Insert the node as a child of this node, and place it at the end of this
     * node's list of children.  If the child already has a parent,
     * this method removes the child from that parent.
     * <p>
     * <b>Note:</b> this method does <i>NOT</i> verify that the child's name is valid within this
     * namespace
     * @param child the child entity
     */
    public synchronized void add( BasicTreeNode child, int index ) {
        Assertion.isNotNull(child,"The child reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(child != this,"The specified node may not be added to itself"); //$NON-NLS-1$
        Assertion.assertTrue(child.getParent() != this,"The specified node is already a child of this entity"); //$NON-NLS-1$
        addChild(child,child.getType(),index);
    }

    protected synchronized void addChild( BasicTreeNode child, ObjectDefinition childType, int index ) {
        // Check if this node can add the new child to its list of children
        if ( this.addChildIsValid(child) ) {
            if ( index == -1 ) {
                this.children.add(child);               // Add at the end
            } else {
                this.children.add(index,child);         // Add at index, may throw IndexOutOfBoundsException
            }
            if ( child.parent != null ) {
                child.parent.children.remove(child);    // doesn't mark node as non-existant
            }
            child.parent = this;                        // remove from parent
            child.setExists(true);                      // ensure that child is marked as exists

            // Check if this node already contains a child that has the same
            // name and type as the new child; if there is an existing child,
            // come up with the next valid name ...
            child.ensureNameIsValid();
        }
    }

    public Object getPropertyValue(PropertyDefinition propertyDefn) {
        Object result = resolveTypeFromString(this.properties.get(propertyDefn),propertyDefn);
        if ( result == null && propertyDefn.hasDefaultValue() ) {
            result = propertyDefn.getDefaultValue();
        }
        if ( result == null ) {
            return result;
        }

        // This is a check to verify that the types correspond to types and that we
        // are no longer using Collection to hold multi-valued properties
        if ( result instanceof Collection && propertyDefn.getPropertyType() != PropertyType.LIST ) {
            throw new AssertionError("The property value is a Collection but the PropertyType is a " + propertyDefn.getPropertyType().getDisplayName() ); //$NON-NLS-1$
        }
        if ( result instanceof Set && propertyDefn.getPropertyType() != PropertyType.SET ) {
            throw new AssertionError("The property value is a Set but the PropertyType is a " + propertyDefn.getPropertyType().getDisplayName() ); //$NON-NLS-1$
        }

        return result;
    }

    public Object setPropertyValue(PropertyDefinition propertyDefn,Object value) {
        Object previous = this.properties.put(propertyDefn,convertTypeToString(value,propertyDefn));
        // Check if the value has changed.  If the old and new value are both null
        // we assume the property was not modified.  If old or new value is not null
        // and does not satisify the .equals check then the value is modified and the
        // node is marked accordingly
        if ( (previous != null && !previous.equals(value)) || (value != null && !value.equals(previous)) ) {
            this.setModified(true,false);
        }
//        if ( previous != value ) {
//            this.setModified(true,false);
//        }
        return previous;
    }

    /**
     * hook to allow some PropertyDefinition values to be converted from String values
     * before they are sent to clients of this Editor.  Subclasses may override
     * this method to convert custom types.
     */
    protected Object resolveTypeFromString(Object value, PropertyDefinition def) {
        Object result = value;
        if ( PropertyType.BOOLEAN.equals(def.getPropertyType()) ) {
            if ( value instanceof String ) {
                if ( Boolean.TRUE.toString().equalsIgnoreCase((String) value) ) {
                    result = Boolean.TRUE;
                } else {
                    result = Boolean.FALSE;
                }
            }
        }
        //TODO: handle other types
        return result;
    }

    /**
     * hook to allow some PropertyDefinition values to be converted non-string values
     * into Strings before they are set on the PropertiedObject.  Subclasses may override
     * this method to convert custom types.
     */
    protected Object convertTypeToString(Object value, PropertyDefinition def) {
        Object result = value;
        if ( PropertyType.BOOLEAN.equals(def.getPropertyType()) ) {
            if ( value instanceof Boolean ) {
                result = value.toString();
            }
        }
        //TODO: handle other types
        return result;
    }

    public Object removePropertyValue(PropertyDefinition propertyDefn) {
        return this.properties.remove(propertyDefn);
    }

    public Map getProperties() {
        return this.properties;
    }

    public void print( PrintStream stream ) {
        Assertion.isNotNull(stream,"The stream reference may not be null"); //$NON-NLS-1$
        printInfo(stream);
        BasicTreeNode child = null;
        Iterator iter = this.iterator();
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            //child.print(stream);
            child.printInfo(stream);
        }
    }

    protected String getEntityForm() {
        return "BasicTreeNode"; //$NON-NLS-1$
    }

    protected void printInfo( PrintStream stream ) {
        // Stream will never be null
        stream.println("                   \"" +  this.getFullName() + "\" <" + this.getEntityForm() + "> (Type=" + this.getType() + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    protected TreeNode find( ObjectID globalUID ) {
        Assertion.isNotNull(globalUID,"The ObjectID reference may not be null"); //$NON-NLS-1$
        if ( this.globalUID.equals(globalUID) ) {
            return this;
        }

        // Go through the children and see if they contain the object ...
        TreeNode result = null;
        BasicTreeNode child = null;
        Iterator iter = this.iterator();
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            result = child.find(globalUID);
            if ( result != null ) {
                break;
            }
        }

        return result;
    }

    /**
     * This method is used to determine the property definitions for this object.
     * By default, this method returns an empty list.
     */
    protected List getPropertyDefinitions() {
        return Collections.EMPTY_LIST;
    }

    /**
     * This method is used to determine which, if any, of the property definitions for this object
     * are used to access the name.
     * By default, this method returns null, meaning there is no name property definition.
     */
    protected PropertyDefinition getNamePropertyDefinition() {
        return null;
    }

    /**
     * This method is used to determine which, if any, of the property definitions for this object
     * are used to access the description.
     * By default, this method returns null, meaning there is no description property definition.
     */
    protected PropertyDefinition getDescriptionPropertyDefinition() {
        return null;
    }

    /**
     * This method is used to determine whether two child nodes under a single parent
     * are allowed to have the same name.  By default, this method returns <code>true</code>.
     */
    protected boolean areDuplicateChildNamesAllowed() {
        return true;
    }

    /**
     * This method is used to determine the default name for a node when that node has no name.
     * By default, this method returns the name of the node's type.
     */
    protected String getDefaultName() {
        return this.type.getName();
    }

    protected boolean isValidNewName(String newName) {
        if ( newName == null || newName.length() == 0 ) {
            return false;
        }
        if ( newName.equals(this.name) ) {
            return true;
        }
        if ( this.parent != null ) {
            if ( this.areDuplicateChildNamesAllowed() ) {
                return true;
            }
            if ( this.parent.containsChildWithName(newName) ) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method is called prior to adding the specified child to this
     * TreeNode allowing any subclass to provide specific behavior if necessary.
     * This implementation returns true by default.
     */
    protected boolean addChildIsValid( BasicTreeNode child ) {
        return true;
    }

    /**
     * Set the name of the new node (before its been added as a sibling to the nodes
     * in <code>siblings</code>
     */
    protected void ensureNameIsValid() {
        if ( this.name.length() == 0 ) {
            this.setNameOfNode( this.getDefaultName() );
        }
        if ( areDuplicateChildNamesAllowed() ) {
            return;            // nothing to rename !!!
        }
        if ( this.parent == null ) {
            return;
        }
        List siblings = this.parent.getChildren();
        if ( siblings.size() == 1 ) {
            return;
        }

        // Determine if there is a conflict ...
        boolean rename = false;
        Iterator iter = siblings.iterator();
        BasicTreeNode sibling = null;
        while ( iter.hasNext() ) {
            sibling = (BasicTreeNode) iter.next();
            if ( sibling != this && sibling.getName().equals(this.name) ) {
                rename = true;
                break;
            }
        }

        if ( !rename ) {
//System.out.println("setValidName(" + newNode.getFullName() + ") is NOT renaming");
            return;
        }

//System.out.println("setValidName(" + newNode.getFullName() + ") IS renaming");
        String newName = null;
        int suffix = 1;
        while ( newName == null ) {
            String potentialName = this.name + suffix;
            iter = siblings.iterator();
            while ( iter.hasNext() ) {
                sibling = (BasicTreeNode) iter.next();
//System.out.println("setValidName(" + newNode.getFullName() + ") : comparing " + potentialName + " to " + sibling.getName() );
                if ( sibling != this && sibling.getName().equals(potentialName) ) {
                    potentialName = null;
//System.out.println("setValidName(" + newNode.getFullName() + ") : breaking " );
                    break;
                }
            }
            if ( potentialName != null ) {
//System.out.println("setValidName(" + newNode.getFullName() + ") : new name = " + potentialName );
                newName = potentialName;
            }
            ++suffix;
        }
        this.setNameOfNode(newName);
    }

}

