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

package com.metamatrix.common.tree.directory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertyAccessPolicy;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.FileUtils;

/**
 * This interface defines a view of a hierarchy of TreeNode instances.
 */
public class FileSystemEntryEditor implements DirectoryEntryEditor {

    private FileSystemView view;

    protected FileSystemEntryEditor( FileSystemView view ) {
        Assertion.isNotNull(view,"The FileSystemView reference may not be null"); //$NON-NLS-1$
        this.view = view;
    }

    protected FileSystemView getView() {
        return this.view;
    }

    protected TreeNode getActualParent( TreeNode possibleParent ) {
        if ( possibleParent != null ) {
            return possibleParent;
        }
        // Will only have 0 root IF AND ONLY IF the root was set manually
        // AND also hiding root; so return the home (which is always non-null
        // when the root is set manually)
        if ( this.getView().getRoots().size() == 0 ) {
            return this.getView().getHome();
        }

        // There is no parent specified but it is ambiguous, so throw exception
        throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0051));
    }

    /**
     * Set the marked state of the TreeNode entry.
     * @param marked the marked state of the entry.
     */
    public void setMarked( TreeNode entry, boolean marked) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        fsEntry.setMarked(marked);
    }

    /**
     * Return the marked state of the specified entry.
     * @return the marked state of the entry.
     */
    public boolean isMarked( TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        return fsEntry.isMarked();
    }

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    public boolean isParentOf(TreeNode parent, TreeNode child) {
        Assertion.isNotNull(parent,"The TreeNode reference for the parent may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(child,"The TreeNode reference for the child may not be null"); //$NON-NLS-1$
        FileSystemEntry parentEntry = this.assertFileSystemEntry(parent);
        FileSystemEntry childEntry  = this.assertFileSystemEntry(child);
        File parentFile = childEntry.getFile().getParentFile();
        return ( parentEntry.getFile().equals(parentFile) );
    }

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    public boolean isAncestorOf(TreeNode ancestor, TreeNode descendent) {
        Assertion.isNotNull(ancestor,"The TreeNode reference for the ancestor may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(descendent,"The TreeNode reference for the descendent may not be null"); //$NON-NLS-1$
        FileSystemEntry ancestorEntry   = this.assertFileSystemEntry(ancestor);
        FileSystemEntry descendentEntry = this.assertFileSystemEntry(descendent);

        File descendentFile = descendentEntry.getFile().getParentFile();
        while( descendentFile != null ) {
            if ( ancestorEntry.getFile().equals(descendentFile) ) {
                return true;
            }
            descendentFile = descendentFile.getParentFile();
        }
        return false;
    }

    /**
     * Create a new instance of a TreeNode under the specified parent and
     * with the specified type.  The name of the new object is created automatically.
     * <p>
     * The resulting node will not exist (see <code>makeExist()</code>)
     * @param parent the TreeNode that is to be the parent of the new tree node object
     * @param type the ObjectDefinition instance that defines the type of tree node
     * object to instantiate.
     * @return the new instance or null if the new instance could not be created.
     * @throws IllegalArgumentException if the parent and the new TreeNode
     * are not compatible
     * @throws AssertionError if <code>parent</code> or <code>type</code>
     * is null
     */
    public TreeNode create(TreeNode parent, ObjectDefinition type) {
        Assertion.isNotNull(parent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(type,"The ObjectDefinition reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry( this.getActualParent(parent) );
        this.assertDescendentOfRoot(parent);

        if (type instanceof FolderDefinition) {
            try {
                String namePrefix = this.getNewFolderName();
                String newName = namePrefix;
                int counter = 0;
                while ( fsEntry.hasChildWithName(newName) ) {
                    newName = namePrefix + (++counter);
                }
                return ( this.createNewEntry(fsEntry, newName, DirectoryEntry.TYPE_FOLDER) );
            } catch (TreeNodeException e) {
            } catch (IOException e) {
            }
        } else if (type instanceof FileDefinition) {
            try {
                String namePrefix = this.getNewFileName();
                String newName = namePrefix;
                int counter = 0;
                while ( fsEntry.hasChildWithName(newName) ) {
                    newName = namePrefix + (++counter);
                }
                return ( this.createNewEntry(fsEntry, newName, DirectoryEntry.TYPE_FILE) );
            } catch (TreeNodeException e) {
            } catch (IOException e) {
            }
        }
        return null;
    }

    /**
     * Create a new instance of a TreeNode under the specified parent,
     * with the specified type and with the specified name
     * <p>
     * The resulting node will not exist (see <code>makeExist()</code>)
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
    public TreeNode create(TreeNode parent, String name, ObjectDefinition type) {
        Assertion.isNotNull(parent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(name,"The name may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(name,"The name may not be zero-length"); //$NON-NLS-1$
        Assertion.isNotNull(type,"The ObjectDefinition reference may not be null"); //$NON-NLS-1$
        DirectoryEntry fsEntry = this.assertFileSystemEntry( this.getActualParent(parent) );
        this.assertDescendentOfRoot(parent);

        if (type instanceof FolderDefinition) {
            try {
                return ( this.createNewEntry(fsEntry, name, DirectoryEntry.TYPE_FOLDER) );
            } catch (TreeNodeException e) {
            } catch (IOException e) {
            }
        } else if (type instanceof FileDefinition) {
            try {
                return ( this.createNewEntry(fsEntry, name, DirectoryEntry.TYPE_FILE) );
            } catch (TreeNodeException e) {
            } catch (IOException e) {
            }
        }
        return null;
    }

    /**
     * Check for the existance of the specified entry, and creates an underlying
     * resource for the entry if one does not exist.
     * @param obj the node to be deleted; may not be null
     * @return true if the entry was successfully created (made to exist); false
     * if the entry already exists or if the entry could not be created
     * @throws AssertionError if <code>obj</code> is null
     */
    public boolean makeExist(DirectoryEntry entry) {
        Assertion.isNotNull(entry,"The DirectoryEntry reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        if ( fsEntry.exists() ) {
            return false;
        }
        boolean result = false;
        ObjectDefinition type = entry.getType();
        File entryFile = fsEntry.getFile();
        try {
            if ( type == DirectoryEntry.TYPE_FOLDER ) {
                result = entryFile.mkdirs();
            } else {
                result = entryFile.createNewFile();
            }
        } catch ( Exception e ) {
            result = false;
        }
        return result;
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
        Assertion.isNotNull(node,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(node);
        return fsEntry.getFile().delete();
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
     * @return true if this paste was successful, or false otherwise.
     * @throws AssertionError if either of <code>original</code> or <code>newParent</code> is null
     */
    public TreeNode paste(TreeNode original, TreeNode newParent, boolean deepCopy) {
        Assertion.isNotNull(original,"The TreeNode reference for the orginal node may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(newParent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        this.assertFileSystemEntry(original);
        this.assertFileSystemEntry( this.getActualParent(newParent) );
        this.assertDescendentOfRoot(newParent);
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
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
    public boolean move(TreeNode node, TreeNode newParent) {
        Assertion.isNotNull(node,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(newParent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(node);
        FileSystemEntry parentEntry = this.assertFileSystemEntry( this.getActualParent(newParent) );
        this.assertDescendentOfRoot(newParent);
        if ( !parentEntry.getFile().isDirectory() ) {
            return false;
        }
        File originalFile = fsEntry.getFile();
       boolean isValid = fsEntry.move(parentEntry);

    	if(isValid){
    	    view.entryMoved(originalFile, fsEntry);
    	}

    	return isValid;
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
        return move(node,newParent);
    }

    /**
     * Moves this TreeNode to the specified location within the ordered list of
     * children for the node's parent.
     * @param child the node to be moved;  may not be null
     * @param newIndex the position that this node will occupy within the ordered list
     * of children for the node's parent.
     * @return true if this node was moved, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>newParent</code> is null
     */
    public boolean moveChild(TreeNode child, int newIndex) {
        Assertion.isNotNull(child,"The TreeNode reference may not be null"); //$NON-NLS-1$
        this.assertFileSystemEntry(child);
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
    }

    /**
     * Copy this TreeNode as a child of the specified new parent.
     * <p>
     * @param obj the node to be copied;  may not be null and must exist
     * @param newParent the node that is to be considered the
     * parent of the copied instance; may not be null
     * @return a reference to the new TreeNode instance or null if the copy operation
     * was unsuccessful.
     * @throws AssertionError if either of <code>obj</code> or <code>newParent</code> is null
     */
    public TreeNode copy(TreeNode node, TreeNode newParent) {
        Assertion.isNotNull(node,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(newParent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(node);
        FileSystemEntry parentEntry = this.assertFileSystemEntry( this.getActualParent(newParent) );
        Assertion.assertTrue(fsEntry.exists(),"The FileSystemEntry \""+fsEntry.getFullName()+"\" must exist on the file system"); //$NON-NLS-1$ //$NON-NLS-2$
        Assertion.assertTrue(parentEntry.exists(),"The FileSystemEntry \""+parentEntry.getFullName()+"\" must exist on the file system"); //$NON-NLS-1$ //$NON-NLS-2$

        this.assertDescendentOfRoot(newParent);
        if ( !parentEntry.getFile().isDirectory() ) {
            return null;
        }
        FileSystemEntry theCopy = (FileSystemEntry) fsEntry.copy(parentEntry);
        if (theCopy != null) {
            theCopy = view.getFileSystemEntry(theCopy.getFile(), theCopy.getType());
        }

        return theCopy;
    }

    /**
     * Renames this TreeNode to the specified new name.  If this entry
     * represents an existing resource, the underlying resource is changed.
     * @return true if this entry was renamed, or false otherwise.
     */
    public boolean rename(TreeNode node, String newName) {
        Assertion.isNotNull(node,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(newName,"The name may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(newName,"The name may not be zero-length"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(node);
        return fsEntry.renameTo(newName);
    }

    /**
     * Obtain the list of PropertyDefinitions that apply to the specified object's type.
     * @param obj the propertied object for which the PropertyDefinitions are
     * to be obtained; may not be null
     * @return an unmodifiable list of the PropertyDefinition objects that
     * define the properties for the object; never null but possibly empty
     * @throws AssertionError if <code>obj</code> is null
     */
    public List getPropertyDefinitions(PropertiedObject obj) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        return fsEntry.getPropertyDefinitions();
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
     * Filter the specified PropertyDefinition instances and return the first
     * definition that is mapped to "the name" property for the tree node.
     * @param obj the tree node; may not be null
     * @return the first PropertyDefinition instance found in the list of
     * PropertyDefinition instances that represents the name property for the object,
     * or null if no such PropertyDefinition is found.
     */
    public PropertyDefinition getNamePropertyDefinition(TreeNode obj) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        return fsEntry.getNamePropertyDefinition();
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
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        return fsEntry.getDescriptionPropertyDefinition();
    }

    /**
     * Determine whether the specified name is valid for a file or folder on the
     * current file system.
     * @param newName the new name to be checked
     * @return true if the name is null or contains no invalid characters for a
     * folder or file, or false otherwise
     */
    public boolean isNameValid( String newName ) {
        return FileUtils.isFilenameValid(newName);
    }

    /**
     * Obtain from the specified PropertiedObject the property value
     * that corresponds to the specified PropertyDefinition.  The return type and cardinality
     * (including whether the value may be null) depend upon the PropertyDefinition.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0", or the NO_VALUE reference if the specified object
     * does not contain the specified PropertyDefinition
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public Object getValue(PropertiedObject obj,PropertyDefinition def) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        return fsEntry.getValue(def);
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
    public boolean isValidValue(PropertiedObject obj,PropertyDefinition def, Object value ) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        return fsEntry.isValidValue(def,value);
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
     */
    public void setValue(PropertiedObject obj,PropertyDefinition def, Object value) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        fsEntry.setValue(def,value);
    }

    /**
      */
    public void setPolicy(PropertyAccessPolicy policy) {
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
    }

    /**
      */
    public PropertyAccessPolicy getPolicy() {
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
    }

	// ########################## PropertyAccessPolicy Methods ###################################

    /**
     * Return whether this editor may be used to set property values on
     * the specified PropertiedObject.
     * @param obj the propertied object; may not be null
     * @return true if the object may not be modified, or false otherwise.
     * @throws AssertionError if <code>obj</code> is null
     */
    public boolean isReadOnly(PropertiedObject obj) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        return fsEntry.isReadOnly();
    }

    public boolean isReadOnly(PropertiedObject obj, PropertyDefinition def) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(obj);
        return fsEntry.isReadOnly();
        //return this.policy.isReadOnly(obj,def);
    }

    public void setReadOnly(PropertiedObject obj, PropertyDefinition def, boolean readOnly) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null"); //$NON-NLS-1$
        //this.policy.setReadOnly(obj,def,readOnly);
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
    }

    public void setReadOnly(PropertiedObject obj, boolean readOnly) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        //this.policy.setReadOnly(obj,readOnly);
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
    }

    public void reset(PropertiedObject obj) {
        Assertion.isNotNull(obj,"The PropertiedObject reference may not be null"); //$NON-NLS-1$
        //this.policy.reset(obj);
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
    }

	// ########################## Implementation Methods ###################################

    protected FileSystemEntry assertFileSystemEntry( TreeNode obj ) {
        Assertion.assertTrue((obj instanceof FileSystemEntry),"The type of TreeNode entry must be a FileSystemEntry"); //$NON-NLS-1$
        FileSystemEntry entry = (FileSystemEntry) obj;
        return entry;
    }

    protected FileSystemEntry assertFileSystemEntry( PropertiedObject obj ) {
        Assertion.assertTrue((obj instanceof FileSystemEntry),"The type of PropertiedObject entry must be a FileSystemEntry"); //$NON-NLS-1$
        FileSystemEntry entry = (FileSystemEntry) obj;
        return entry;
    }

    protected void assertDescendentOfRoot( TreeNode descendent ) {
        if ( this.getView().isRoot(descendent) ) {
            return;
        }
        Iterator itr = this.getView().getActualRoots().iterator();
        while (itr.hasNext()) {
            TreeNode root = (TreeNode)itr.next();
            if (this.isAncestorOf(root, descendent)) {
                return;
            }
        }
        throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0053));
    }
    /**
     * Create a new resource of the specified type under the specified parent DirectoryEntry.
     * @param parent the DirectoryEntry instance under which the new file resource
     * is to be created; may not be null and must represent a folder entry.
     * @param name the new name for the file resource.
     * @return the new DirectoryEntry that represents the new resource or null if
     * one did not exists and could not be created.
     * @throws TreeNodeException if the underlying resource could not be created
     * @throws IOException if there was an error during the creation
     */
    private TreeNode createNewEntry(TreeNode parent, String name, ObjectDefinition type) throws IOException, TreeNodeException {
        Assertion.isNotNull(parent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(name,"The name may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(name,"The name may not be zero-length"); //$NON-NLS-1$
        Assertion.isNotNull(type,"The ObjectDefinition reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue( ((type instanceof FileDefinition)||(type instanceof FolderDefinition)),"The ObjectDefinition must be of type FileDefinition or FolderDefinition"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(parent);
        File parentFile = fsEntry.getFile();
        File file = new File(parentFile,name);
        return this.view.getFileSystemEntry(file,type);
    }

    protected String getNewFolderName() {
        return "New Folder"; //$NON-NLS-1$
    }

    protected String getNewFileName() {
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0052));
    }
}
