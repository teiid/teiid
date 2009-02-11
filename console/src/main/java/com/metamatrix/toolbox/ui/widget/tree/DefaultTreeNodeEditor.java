/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.tree;

// System imports
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertyAccessPolicy;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.transaction.TransactionStatus;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;

/**
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTreeNodeEditor
implements TreeNodeEditor {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
//    private DefaultTreeNode root;
    private HashMap childrenMap;
    private HashMap parentMap;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNodeEditor(final DefaultTreeNode root, final HashMap parentMap, final HashMap childrenMap) {
//        this.root = root;
        this.parentMap = parentMap;
        this.childrenMap = childrenMap;
        initializeDefaultTreeEditor();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNode create(final TreeNode parent, final ObjectDefinition type) {
        return create(parent, null, type);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNode create(final TreeNode parent, String childName, final ObjectDefinition type) {
        final DefaultTreeNode dfltParent = toDefaultTreeNode(parent);
        if (childName == null) {
            childName = type.getDisplayName();
        }
        final DefaultTreeNode child = new DefaultTreeNode(childName);
        if (childrenMap != null  &&  childrenMap.containsKey(dfltParent)) {
            List children = (List)childrenMap.get(dfltParent);
            if (children == null) {
                children = new ArrayList();
                childrenMap.put(dfltParent, children);
            }
            children.add(child);
            if (parentMap == null) {
                parentMap = new HashMap();
            }
            parentMap.put(child, parent);
        } else {
            dfltParent.addChild(child);
        }
        return child;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public UserTransaction createReadTransaction() {
        return new Transaction();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public UserTransaction createWriteTransaction() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public UserTransaction createWriteTransaction(final Object source) {
        return new Transaction(source);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean delete(final TreeNode node) {
        final DefaultTreeNode dfltNode = toDefaultTreeNode(node);
        // Add map stuff later
        return dfltNode.getParent().removeChild(dfltNode) >= 0;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public PropertyAccessPolicy getPolicy() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getPropertyDefinitions(final PropertiedObject object) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getAllowedValues(final PropertiedObject object, final PropertyDefinition def) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public PropertyDefinition getNamePropertyDefinition(TreeNode obj) {
        return null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Object getValue(final PropertiedObject object, final PropertyDefinition definition) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeDefaultTreeEditor() {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isAncestorOf(final TreeNode ancestor, final TreeNode descendent) {
        final DefaultTreeNode dfltAncestor = toDefaultTreeNode(ancestor);
        final DefaultTreeNode dfltDescendent = toDefaultTreeNode(descendent);
        // Add map stuff later
        return dfltDescendent.isDescendentOf(dfltAncestor);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isMarked(final TreeNode node) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isParentOf(final TreeNode parent, final TreeNode child) {
        final DefaultTreeNode dfltChild = toDefaultTreeNode(child);
        // Add map stuff later
        return dfltChild.getParent() == parent;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isReadOnly(final PropertiedObject object) {
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isReadOnly(final PropertiedObject object, final PropertyDefinition definition) {
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isValidValue(final PropertiedObject object, final PropertyDefinition definition, final Object value) {
        return true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean move(final TreeNode child, final TreeNode parent) {
        final DefaultTreeNode dfltChild = toDefaultTreeNode(child);
        final DefaultTreeNode dfltParent = toDefaultTreeNode(parent);
        // Add map stuff later
        if (dfltChild.getParent().removeChild(dfltChild) < 0) {
            return false;
        }
        return dfltParent.addChild(child) >= 0;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean move(TreeNode child, TreeNode parent, int index) {
        final DefaultTreeNode dfltChild = toDefaultTreeNode(child);
        final DefaultTreeNode dfltParent = toDefaultTreeNode(parent);
        // Add map stuff later
        if (dfltChild.getParent().removeChild(dfltChild) < 0) {
            return false;
        }
        return dfltParent.addChild(child, index);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean moveChild(final TreeNode child, final int index) {
        final DefaultTreeNode dfltChild = toDefaultTreeNode(child);
        // Add map stuff later
        final DefaultTreeNode parent = dfltChild.getParent();
        final int oldNdx = parent.removeChild(dfltChild);
        if (oldNdx < 0) {
            return false;
        }
        if (oldNdx >= index) {
            return parent.addChild(dfltChild, index);
        }
        return parent.addChild(dfltChild, index - 1);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNode paste(final TreeNode child, final TreeNode parent, final boolean isCopyDeep) {
        DefaultTreeNode dfltChild = toDefaultTreeNode(child);
        final DefaultTreeNode dfltParent = toDefaultTreeNode(parent);
        // Add map stuff later
        try {
            dfltChild = (DefaultTreeNode)dfltChild.clone();
            dfltParent.addChild(dfltChild);
        } catch (final CloneNotSupportedException notPossible) {}
        return dfltChild;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean rename(final TreeNode node, final String name) {
        if (!(node instanceof DefaultTreeNode)) {
            throw new IllegalArgumentException("The node parameter must be an instance of DefaultTreeNode");
        }
        ((DefaultTreeNode)node).setName(name);
        return true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void reset(final PropertiedObject object) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setMarked(final TreeNode node, final boolean isMarked) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setPolicy(final PropertyAccessPolicy policy) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setReadOnly(final PropertiedObject object, final boolean isReadOnly) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setReadOnly(final PropertiedObject object, final PropertyDefinition definition, final boolean isReadOnly) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setValue(final PropertiedObject object, final PropertyDefinition definition, final Object value) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    private DefaultTreeNode toDefaultTreeNode(final TreeNode node) {
        if (!(node instanceof DefaultTreeNode)) {
            throw new IllegalArgumentException("TreeNode parameters must be an instance of DefaultTreeNode");
        }
        return (DefaultTreeNode)node;
    }

    //############################################################################################################################
    //# Inner Class: Transaction                                                                                                 #
    //############################################################################################################################
    
    private class Transaction
    implements UserTransaction {
        //# Transaction ##########################################################################################################
        //# Instance Variables                                                                                                   #
        //########################################################################################################################
        
        private Object src = null;
        
        //# Transaction ##########################################################################################################
        //# Constructors                                                                                                         #
        //########################################################################################################################
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        private Transaction() {
            this(null);
        }
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        private Transaction(final Object source) {
            src = source;
        }
        
        //# Transaction ##########################################################################################################
        //# Instance Methods                                                                                                     #
        //########################################################################################################################
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        public void begin() {
        }
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        public void commit() {
        }
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        public Object getSource() {
            return src;
        }
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        public int getStatus() {
            return TransactionStatus.STATUS_UNKNOWN;
        }
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        public void rollback() {
        }
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        public void setRollbackOnly() {
        }
        
        // Transaction ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since 2.0
        */
        public void setTransactionTimeout(final int seconds) {
        }
    }
}
