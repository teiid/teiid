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

package com.metamatrix.common.tree;

import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;

/**
 * This interface defines a view of a hierarchy of TreeNode instances.
 */
public interface TreeNodeEditor extends PropertiedObjectEditor {

    /**
     * Set the marked state of the TreeNode node.
     * @param marked the marked state of the node.
     */
    void setMarked(TreeNode node, boolean marked);

    /**
     * Return the marked state of the specified node.
     * @return the marked state of the node.
     */
    boolean isMarked(TreeNode node);

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    boolean isParentOf(TreeNode parent, TreeNode child);

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    boolean isAncestorOf(TreeNode ancestor, TreeNode descendent);

    /**
     * Create a new instance of a TreeNode under the specified parent and
     * with the specified type.  The name of the new object is created automatically.
     * @param parent the TreeNode that is to be the parent of the new tree node object
     * @param type the ObjectDefinition instance that defines the type of tree node
     * object to instantiate.
     * @return the new instance or null if the new instance could not be created.
     * @throws IllegalArgumentException if the parent and the new TreeNode
     * are not compatible
     * @throws AssertionError if <code>parent</code> or <code>type</code>
     * is null
     */
    TreeNode create(TreeNode parent, ObjectDefinition type);

    /**
     * Create a new instance of a TreeNode under the specified parent,
     * with the specified type and with the specified name
     * @param parent the TreeNode that is to be the parent of the new tree node object
     * @param name the name for the new object
     * @param type the ObjectDefinition instance that defines the type of tree node
     * object to instantiate.
     * @return the new instance or null if the new instance could not be created.
     * @throws IllegalArgumentException if the parent and the new TreeNode
     * are not compatible
     * @throws AssertionError if <code>parent</code> or <code>type</code>
     * is null
     */
    TreeNode create(TreeNode parent, String name, ObjectDefinition type);

    /**
     * Removes the specified TreeNode instance (and all its children) from
     * its parent.  After this method is called, the caller is responsible for
     * maintaining the referenced to the specified object (to prevent garbage
     * collection).
     * @param obj the node to be deleted; may not be null
     * @return true if deletion is successful, or false otherwise.
     * @throws AssertionError if <code>obj</code> is null
     */
    boolean delete(TreeNode node);

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
    TreeNode paste(TreeNode original, TreeNode newParent, boolean deepCopy);

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
    boolean move(TreeNode node, TreeNode newParent);

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
    boolean move(TreeNode node, TreeNode newParent, int indexInNewParent);

    /**
     * Moves this TreeNode to the specified location within the ordered list of
     * children for the node's parent.
     * @param child the node to be moved;  may not be null
     * @param newIndex the position that this node will occupy within the ordered list
     * of children for the node's parent.
     * @return true if this node was moved, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>newParent</code> is null
     * @throws IndexOutOfBoundsException if the index is not within the range
     * <code>0 <= newIndex < childCount</code>
     */
    boolean moveChild(TreeNode child, int newIndex);

    /**
     * Renames this TreeNode to the specified new name.  This operation will
     * not succeed if there is already a node underneath the parent
     * with the same name as the specified newName.
     * @param obj the node to be renamed; may not be null
     * @param newName the new name for the object; may not be null or zero-length,
     * and must not be used by an existing sibling
     * @return true if this node was renamed, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>newName</code> is null,
     * or if <code>newName</code> is zero-length
     */
    boolean rename(TreeNode node, String newName);

    /**
     * Filter the specified PropertyDefinition instances and return the first
     * definition that is mapped to "the name" property for the tree node.
     * @param obj the tree node; may not be null
     * @return the first PropertyDefinition instance found in the list of
     * PropertyDefinition instances that represents the name property for the object,
     * or null if no such PropertyDefinition is found.
     */
    PropertyDefinition getNamePropertyDefinition(TreeNode obj);

    /**
     * Filter the specified PropertyDefinition instances and return the first
     * definition that is mapped to "the description" property for the metadata object.
     * @param obj the tree node; may not be null
     * @return the first PropertyDefinition instance found in the list of
     * PropertyDefinition instances that represents the description property for the object,
     * or null if no such PropertyDefinition is found.
     */
//    PropertyDefinition getDescriptionPropertyDefinition(TreeNode obj);

}
