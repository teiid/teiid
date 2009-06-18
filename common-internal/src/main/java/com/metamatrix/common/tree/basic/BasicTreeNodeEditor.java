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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.DefaultPropertyAccessPolicy;
import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertyAccessPolicy;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.id.ObjectIDFactory;
import com.metamatrix.core.util.Assertion;

public class BasicTreeNodeEditor implements TreeNodeEditor {

    private ObjectIDFactory idFactory;
    private PropertyAccessPolicy policy;

    public BasicTreeNodeEditor( ObjectIDFactory idFactory, PropertyAccessPolicy policy ) {
        Assertion.isNotNull(idFactory,"The ObjectIDFactory reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(policy,"The PropertyAccessPolicy reference may not be null"); //$NON-NLS-1$
        this.idFactory = idFactory;
        this.policy = policy;
    }

    /**
     * Create an empty property definition object with all defaults.
     */
    public BasicTreeNodeEditor( ObjectIDFactory idFactory ) {
        this(idFactory, new DefaultPropertyAccessPolicy() );
    }

    protected BasicTreeNode assertBasicTreeNode( PropertiedObject obj ) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(obj instanceof BasicTreeNode,"The referenced object must be an instance of BasicTreeNode"); //$NON-NLS-1$
        return (BasicTreeNode) obj;
    }

	// ########################## PropertiedObjectEditor Methods ###################################

    /**
     * Obtain the list of PropertyDefinitions that apply to the specified object's type.
     * @param obj the propertied object for which the PropertyDefinitions are
     * to be obtained; may not be null
     * @return an unmodifiable list of the PropertyDefinition objects that
     * define the properties for the object; never null but possibly empty
     * @throws AssertionError if <code>obj</code> is null
     */
    public List getPropertyDefinitions(PropertiedObject obj) {
        BasicTreeNode entity = assertBasicTreeNode(obj);
        return entity.getPropertyDefinitions();
    }

    /**
     * Get the allowed values for the property on the specified object.
     * By default, this implementation simply returns the allowed values in the
     * supplied PropertyDefinition instance.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the unmodifiable list of allowed values for this property, or an empty
     * set if the values do not have to conform to a fixed set.
     * @see #hasAllowedValues
     */
    public List getAllowedValues(PropertiedObject obj, PropertyDefinition def) {
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        return def.getAllowedValues();
    }

    /**
     * Obtain from the specified PropertiedObject the property value
     * that corresponds to the specified PropertyDefinition.  The return type and cardinality
     * (including whether the value may be null) depend upon the PropertyDefinition.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which may be an empty collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0", or the NO_VALUE reference if the specified object
     * does not contain the specified PropertyDefinition
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public Object getValue(PropertiedObject obj, PropertyDefinition def) {
        BasicTreeNode entity = assertBasicTreeNode(obj);
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        return entity.getPropertyValue(def);
    }

    /**
     * Return whether the specified value is considered valid.  The value is not
     * valid if the propertied object does not have the specified property definition,
     * or if it does but the value is inconsistent with the requirements of the
     * property definition.
     * @param obj the propertied object whose property value is to be validated;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be validated; may not be null
     * @param value the proposed value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0"
     * @return true if the value is considered valid, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public boolean isValidValue(PropertiedObject obj, PropertyDefinition def, Object value ) {
        BasicTreeNode entity = assertBasicTreeNode(obj);
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$

        // Check for a null value ...
        if ( value == null ) {
            return ( !def.isRequired() ); // only if minimum==0 is value allowed to be null
        }
        // From this point forward, the value is never null

        // Check if the property definition is the name ...
        if ( entity.getNamePropertyDefinition() == def ) {
            if ( !(value instanceof String) ) {
                return false;
            }
            if ( !entity.isValidNewName(value.toString()) ) {
                return false;
            }
            return true;
        }
        return def.getPropertyType().isValidValue(value);
    }

    /**
     * Set on the specified PropertiedObject the value defined by the specified PropertyDefinition.
     * @param obj the propertied object whose property value is to be set;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be changed; may not be null
     * @param value the new value for the property; the cardinality and type
     * must conform PropertyDefinition
     * @throws IllegalArgumentException if the value does not correspond
     * to the PropertyDefinition requirements.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     * @throws RuntimeException if attempting to update a read-only value.
     */
    public void setValue(PropertiedObject obj, PropertyDefinition def, Object value) {
        BasicTreeNode entity = assertBasicTreeNode(obj);
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        if ( this.isReadOnly(obj,def) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0023));
        }

// TODO: Do we need to validate the value?

        // Set the property value ...
        entity.setPropertyValue(def,value);

        // If it is the name property definition, rename the node ...
        if ( entity.getNamePropertyDefinition() == def ) {
            entity.setName(value.toString());
        }
    }

    public PropertyAccessPolicy getPolicy() {
        return this.policy;
    }

    public void setPolicy(PropertyAccessPolicy policy) {
        if ( policy == null ) {
            this.policy = new DefaultPropertyAccessPolicy();
        } else {
            this.policy = policy;
        }
    }


	// ########################## PropertyAccessPolicy Methods ###################################

    public boolean isReadOnly(PropertiedObject obj) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        return this.policy.isReadOnly(obj);
    }

    public boolean isReadOnly(PropertiedObject obj, PropertyDefinition def) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        return this.policy.isReadOnly(obj,def);
    }

    public void setReadOnly(PropertiedObject obj, PropertyDefinition def, boolean readOnly) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        this.policy.setReadOnly(obj,def,readOnly);
    }

    public void setReadOnly(PropertiedObject obj, boolean readOnly) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        this.policy.setReadOnly(obj,readOnly);
    }

    public void reset(PropertiedObject obj) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        this.policy.reset(obj);
    }

	// ########################## TreeEditor Methods ###################################

    /**
     * Set the marked state of the TreeNode node.
     * @param marked the marked state of the node.
     */
    public void setMarked(TreeNode node, boolean marked) {
        BasicTreeNode entity = assertBasicTreeNode(node);
        entity.setMarked(marked);
    }

    /**
     * Return the marked state of the specified node.
     * @return the marked state of the node.
     */
    public boolean isMarked(TreeNode node) {
        BasicTreeNode entity = assertBasicTreeNode(node);
        return entity.isMarked();
    }

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    public boolean isParentOf(TreeNode parent, TreeNode child) {
        BasicTreeNode parentEntity = assertBasicTreeNode(parent);
        BasicTreeNode childEntity = assertBasicTreeNode(child);
        return parentEntity.isParentOf(childEntity);
    }

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    public boolean isAncestorOf(TreeNode ancestor, TreeNode descendent) {
        BasicTreeNode ancestorEntity = assertBasicTreeNode(ancestor);
        BasicTreeNode descendentEntity = assertBasicTreeNode(descendent);
        return ancestorEntity.isAncestorOf(descendentEntity);
    }

    /**
     * Create a new instance of a TreeNode under the specified parent and
     * with the specified type.  The name of the new object is created automatically.
     * @param parent the MetaObject that is to be the parent of the new metadata object
     * @param type the ObjectDefinition instance that defines the type of metadata
     * object to instantiate.
     * @return the new instance
     * @throws IllegalArgumentException if the parent and the new MetaObject
     * are not compatible
     * @throws AssertionError if <code>parent</code> or <code>type</code>
     * is null
     */
    public TreeNode create(TreeNode parent, ObjectDefinition type ) {
        return create(parent,null,type);
    }

    /**
     * Create a new instance of a TreeNode under the specified parent,
     * with the specified type and with the specified name
     * @param parent the MetaObject that is to be the parent of the new metadata object
     * @param name the name for the new object
     * @param type the ObjectDefinition instance that defines the type of metadata
     * object to instantiate.
     * @return the new instance
     * @throws AssertionError if <code>parent</code> or <code>type</code>
     * is null
     */
    public TreeNode create(TreeNode parent, String name, ObjectDefinition type ) {
        if ( this.isReadOnly(parent) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0023));
        }
        BasicTreeNode parentEntity = assertBasicTreeNode(parent);

        // Create the new instance ...
        ObjectID guid = this.idFactory.create();
        BasicTreeNode newChild = new BasicTreeNode(parentEntity,name,type,guid);
        return newChild;
    }

    /**
     * Removes the specified TreeNode instance (and all its children) from
     * its parent.  After this method is called, the caller is responsible for
     * maintaining the referenced to the specified object (to prevent garbage
     * collection).
     * @param obj the node to be deleted; may not be null
     * @return true if deletion is successful, or false otherwise.
     * @throws AssertionError if <code>obj</code> is null
     */
    public boolean delete(TreeNode node) {
        if ( this.isReadOnly(node) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0023));
        }
        BasicTreeNode nodeEntity = assertBasicTreeNode(node);  // allow only ModelEntity objects to be deleted
        BasicTreeNode parentEntity = nodeEntity.getParent();
        parentEntity.getIndexOfChild(nodeEntity);
        parentEntity.remove(nodeEntity);        // marks it as non-existant

// TODO: Find all references to the deleted entities and clean them up

        return true;
    }

    /**
     * Creates and places a copy of the specified original TreeNode under the specified new parent.
     * This method does not affect the original TreeNode or its contents.
     * <p>
     * This methods may be used in conjunction with a reference
     * to an existing TreeNode instance in this or another session to <i>copy</i>
     * TreeNode instances and paste them in this session.
     * @param original the original node to be copied;  may not be null
     * @param newParent the nodethat is to be considered the
     * parent of the newly created instances; may not be null
     * @param deepCopy true if this paste operation is to place a deep copy of
     * <code>original</code>, or false if only the <code>original</code>
     * node and its immediate properties are to be pasted.
     * @return the node that resulted from the paste, or null if the paste failed.
     * @throws AssertionError if either of <code>original</code> or <code>newParent</code> is null
     */
    public TreeNode paste(TreeNode original, TreeNode newParent, boolean deepCopy) {
        return paste(original,newParent,deepCopy,false);
    }

    /**
     * Creates and places a copy of the specified original TreeNode under the
     * specified new parent. This method does not affect the original TreeNode
     * or its contents.
     * <p>
     * This methods may be used in conjunction with a reference
     * to an existing TreeNode instance in this or another session to <i>copy</i>
     * TreeNode instances and paste them in this session.
     * @param original the original node to be copied;  may not be null
     * @param newParent the nodethat is to be considered the
     * parent of the newly created instances; may not be null
     * @return the node that resulted from the paste, or null if the paste failed.
     * @throws AssertionError if either of <code>original</code> or <code>newParent</code> is null
     */
    public TreeNode paste(TreeNode original, TreeNode newParent) {
        return paste(original,newParent,true,false);
    }

    /**
     * Creates and places a copy of the specified original TreeNode under the specified new parent.
     * This method does not affect the original TreeNode or its contents.
     * <p>
     * This methods may be used in conjunction with a reference
     * to an existing TreeNode instance in this or another session to <i>copy</i>
     * TreeNode instances and paste them in this session.
     * @param original the original node to be copied;  may not be null
     * @param newParent the nodethat is to be considered the
     * parent of the newly created instances; may not be null
     * @param deepCopy true if this paste operation is to place a deep copy of
     * <code>original</code>, or false if only the <code>original</code>
     * node and its immediate properties are to be pasted.
     * @param forceCopy if true, ensures that original objects are never reused but always
     * copied; if false, this method may choose whether to create copies or to reuse the original.
     * @return the node that resulted from the paste, or null if the paste failed.
     * @throws AssertionError if either of <code>original</code> or <code>newParent</code> is null
     */
    public TreeNode paste(TreeNode original, TreeNode newParent, boolean deepCopy, boolean forceCopy) {
        Assertion.assertTrue(deepCopy == true, "Only a deep copy operations are allowed"); //$NON-NLS-1$
        if ( this.isReadOnly(newParent) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0024, newParent));
        }
        BasicTreeNode originalEntity = assertBasicTreeNode(original);
        BasicTreeNode newParentEntity = assertBasicTreeNode(newParent);
        BasicTreeNode newChildEntity = originalEntity;    // assume we'll use the original

        // Make a copy IF the original exists or it is forced
        if ( forceCopy || original.exists() ) {
            newChildEntity = cloneBasicTreeNode( originalEntity, null, deepCopy );
        }

        // Add the original (or the copy) to the new parent ...
        newParentEntity.add(newChildEntity);
        return newChildEntity;
    }

    /**
     * Moves this TreeNode to be a child of the specified new parent.
     * The specified object <i>is</i> modified, since it's namespace is changed
     * to be newParent.
     * <p>
     * This method may be used in conjunction with the <code>delete</code> method
     * of an editor from this or another another session to <i>cut</i> TreeNode instances
     * from the original's session and paste them (or move them) into this session.
     * <p>
     * @param obj the node to be moved;  may not be null
     * @param newParent the node that is to be considered the
     * parent of the existing instance; may not be null
     * @return true if this node was moved, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>newParent</code> is null
     */
    public boolean move(TreeNode original, TreeNode newParent) {
        if ( this.isReadOnly(original) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0023));
        }
        BasicTreeNode originalEntity = assertBasicTreeNode(original);
        BasicTreeNode newParentEntity = assertBasicTreeNode(newParent);
        BasicTreeNode originalParentEntity = originalEntity.getParent();
        if ( newParent == originalParentEntity ) {
            return false;
        }

//        if ( originalParentEntity != null ) {
//            originalParentEntity.getIndexOfChild(originalEntity);
//        }

        // Add the original by moving to the new parent ...
        newParentEntity.add(originalEntity);
        return true;
    }

    /**
     * Moves this TreeNode to be a child at a particular index in the specified new parent.
     * The specified object <i>is</i> modified, since it's namespace is changed
     * to be newParent.
     * <p>
     * This method may be used in conjunction with the <code>delete</code> method
     * of an editor from this or another another session to <i>cut</i> TreeNode instances
     * from the original's session and paste them (or move them) into this session.
     * <p>
     * @param obj the node to be moved;  may not be null
     * @param newParent the node that is to be considered the
     * parent of the existing instance; may not be null
     * @param indexInNewParent the position that this node will occupy within the ordered list
     * of children for newParent.
     * @return true if this node was moved, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>newParent</code> is null
     * @throws IndexOutOfBoundsException if the index is not within the range
     * <code>0 <= newIndex < childCount</code>
     */
    public boolean move(TreeNode node, TreeNode newParent, int indexInNewParent) {
        if ( this.isReadOnly(node) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0023));
        }
        BasicTreeNode originalEntity = assertBasicTreeNode(node);
        BasicTreeNode newParentEntity = assertBasicTreeNode(newParent);
        BasicTreeNode originalParentEntity = originalEntity.getParent();
        if ( newParent == originalParentEntity && indexInNewParent != originalParentEntity.getIndexOfChild(originalEntity) ) {
            return moveChild(node,indexInNewParent);
        }

//        int indexInOldParent = -1;
//        if ( originalParentEntity != null ) {
//            indexInOldParent = originalParentEntity.getIndexOfChild(originalEntity);
//        }

        // Add the original by moving to the new parent ...
        newParentEntity.add(originalEntity,indexInNewParent);
        return true;
    }

    /**
     * Moves this TreeNode to the specified location within the ordered list of
     * children for the node's parent.
     * @param child the node to be moved;  may not be null
     * @param newIndex the position that this node will occupy within the ordered list
     * of children for the node's parent.
     * @return true if this node was moved, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>newParent</code> is null
     * @throws IndexOutOfBoundsException if the index is not within the range
     * <code>0 <= newIndex < this.getChildCount()</code>
     */
    public boolean moveChild(TreeNode child, int newIndex) {
        if ( this.isReadOnly(child) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0023));
        }
        BasicTreeNode childEntity = assertBasicTreeNode(child);
        BasicTreeNode parentEntity = childEntity.getParent();

        // If there is no parent, then simply return ...
        if ( parentEntity == null ) {
            if ( newIndex != 0 ) {
                throw new IndexOutOfBoundsException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0025));
            }
            return false;
        }
        parentEntity.moveChild(childEntity,newIndex);     // may throw IndexOutOfBoundsException
        return true;
    }

    /**
     * Renames this MetaObject to the specified new name.  This operation will
     * not succeed if there is already a node underneath the parent
     * with the same name as the specified newName.
     * @param obj the node to be renamed; may not be null
     * @param newName the new name for the object; may not be null or zero-length,
     * and must not be used by an existing sibling
     * @return true if this node was renamed, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>newName</code> is null,
     * or if <code>newName</code> is zero-length
     */
    public boolean rename(TreeNode node, String newName) {
        if ( this.isReadOnly(node) ) {
            throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0023));
        }
        BasicTreeNode childEntity = assertBasicTreeNode(node);

        // Make sure the name is valid
        if ( !childEntity.isValidNewName(newName) ) {
            return false;
        }
        childEntity.setName(newName);
        return true;
    }

    /**
     * Filter the specified PropertyDefinition instances and return the first
     * definition that is mapped to "the name" property for the tree node.
     * @param obj the tree node; may not be null
     * @return the first PropertyDefinition instance found in the list of
     * PropertyDefinition instances that represents the name property for the object,
     * or null if no such PropertyDefinition is found.
     */
    public PropertyDefinition getNamePropertyDefinition(TreeNode obj) {
        BasicTreeNode entity = assertBasicTreeNode(obj);
        return entity.getNamePropertyDefinition();
    }

    /**
     * Filter the specified PropertyDefinition instances and return the first
     * definition that is mapped to "the description" property for the metadata object.
     * @param obj the tree node; may not be null
     * @return the first PropertyDefinition instance found in the list of
     * PropertyDefinition instances that represents the description property for the object,
     * or null if no such PropertyDefinition is found.
     */
    public PropertyDefinition getDescriptionPropertyDefinition(TreeNode obj) {
        BasicTreeNode entity = assertBasicTreeNode(obj);
        return entity.getDescriptionPropertyDefinition();
    }

    protected BasicTreeNode cloneBasicTreeNode( BasicTreeNode original, BasicTreeNode newParent, boolean deep ) {
        // Create a copy of the original ...
        ObjectID guid = this.idFactory.create();
        BasicTreeNode copy = new BasicTreeNode(newParent,original.getName(),original.getType(),guid);

        // Add add copies of all of the properties ...
        Map originalProps = original.getProperties();
        Iterator iter = originalProps.entrySet().iterator();
        while ( iter.hasNext() ) {
            Map.Entry entry = (Map.Entry) iter.next();
            PropertyDefinition key = (PropertyDefinition) entry.getKey();
            Object value = entry.getValue();
            copy.setPropertyValue(key,copyPropertyValue(value));
        }

        if ( deep ) {
            iter = original.iterator();
            while ( iter.hasNext() ) {
                BasicTreeNode child = (BasicTreeNode) iter.next();
                cloneBasicTreeNode(child,copy,deep);
            }
        }

        return copy;
    }

    protected Object copyPropertyValue( Object value ) {
        if ( value == null ) {
            return value;
        }
        if ( value instanceof String ) {
            return value.toString();        // don't copy Strings, since they are immutable and can be reused
        }
        if ( value instanceof ObjectID ) {
            return value;                   // don't copy ObjectIDs, since they are immutable and can be reused
        }
        if ( value instanceof DirectoryEntry ) {
            return value;                   // don't copy DirectoryEntry, since they can be reused
        }
        if ( value instanceof List ) {
            List valueList = (List) value;
            List result = new ArrayList(valueList.size());
            Iterator iter = valueList.iterator();
            while ( iter.hasNext() ) {
                result.add( copyPropertyValue(iter.next()) );
            }
            return result;
        }
        throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0026, value.getClass().getName()));
    }

}

