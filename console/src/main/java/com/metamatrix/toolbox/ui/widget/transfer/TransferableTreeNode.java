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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.transfer;

// JDK imports
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.util.List;

import com.metamatrix.common.tree.TreeNode;

/**
@since 2.0
@version 2.0
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class TransferableTreeNode
implements Transferable {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    static final DataFlavor[] FLAVORS = {
        new Flavor(DataFlavor.javaJVMLocalObjectMimeType, "Tree Node")
    };

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private TreeNode node;
    private List nodes;
//    private boolean overValidDropTarget;
     
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TransferableTreeNode(final TreeNode node) {
        initializeTransferableTreeNode(node);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TransferableTreeNode(final List nodes) {
        initializeTransferableTreeNode(nodes);
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Object getTransferData(final DataFlavor flavor)
    throws UnsupportedFlavorException {
        if (isDataFlavorSupported(flavor)) {
            if ( node != null ) {
                return node;
            }
            return nodes;
        }
        throw new UnsupportedFlavorException(flavor);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DataFlavor[] getTransferDataFlavors() {
        return (DataFlavor[])FLAVORS.clone();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeTransferableTreeNode(final List nodes) {
        this.nodes = nodes;
        ((Flavor)FLAVORS[0]).setTransferable(this);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeTransferableTreeNode(final TreeNode node) {
        this.node = node;
        ((Flavor)FLAVORS[0]).setTransferable(this);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isDataFlavorSupported(final DataFlavor flavor) {
        for (int ndx = FLAVORS.length; --ndx >= 0;) {
            if (flavor.equals(FLAVORS[ndx])) {
                return true;
            }
        }
        return false;
    }
}
