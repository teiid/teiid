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

//#############################################################################
package com.metamatrix.console.ui.tree;

import java.util.List;

import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

/**
 * The <code>SortableChildrenNode</code> is a tree node whose children will be sorted
 * based on their <code>toString</code> implementation.
 * @since Golden Gate
 * @version 1.0
 * @author Dan Florian
 */
public class SortableChildrenNode
    extends DefaultTreeNode {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Constructs a tree node whose children will be sorted.
     * @param theUserObject the user object
     */
    public SortableChildrenNode(Object theUserObject) {
        super(theUserObject);
    }

    /**
     * Constructs a tree node whose children will be sorted.
     * @param theName the text used by the <code>toString</code> method
     * @param theUserObject the user object
     */
    public SortableChildrenNode(
        String theName,
        Object theUserObject) {

        super(theName, theUserObject);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Adds the given child tree node in the proper sorted position.
     * @param theChildNode the child node being added
     * @param the index the child node was added at
     */
    public int addChild(Object theChildNode) {
        int index = -1;
        if (getChildCount() > 0) {
            List kids = getChildren();
            for (int numKids=kids.size(), i=0; i<numKids; i++) {
                Comparable kid = (Comparable)kids.get(i);
                if (kid.compareTo(theChildNode) > 0) {
                    addChild(theChildNode, i);
                    index = i;
                    break;
                }
            }
            if (index == -1) {
                index = super.addChild(theChildNode);
            }

        }
        else {
            index = super.addChild(theChildNode);
        }
        return index;
    }
}

