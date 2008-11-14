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
package com.metamatrix.toolbox.ui.widget.transfer;

// JDK imports
import java.awt.datatransfer.DataFlavor;
import java.awt.dnd.DnDConstants;
import java.awt.dnd.DragGestureEvent;
import java.awt.dnd.DragSource;
import java.awt.dnd.DragSourceDragEvent;
import java.awt.dnd.DragSourceDropEvent;
import java.awt.dnd.DragSourceEvent;
import java.awt.dnd.DropTargetDragEvent;
import java.awt.dnd.DropTargetDropEvent;
import java.awt.dnd.DropTargetEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.core.util.Assertion;

/**
@since 2.1
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public abstract class AbstractTreeNodeDragAndDropController extends AbstractDragAndDropController
implements TreeNodeDragAndDropController {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################
    
	protected static transient TreeNode dragParent, dropParent;
    protected static transient int dropNdx, dragNdx;
    protected static transient TransferableTreeNode xferable;
    protected static transient boolean validDragSrc, validDropTarget;

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
	public static int getDragIndex() {
        return dragNdx;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public static TreeNode getDragParent() {
        return dragParent;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public static int getDropIndex() {
        return dropNdx;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public static TreeNode getDropParent() {
        return dropParent;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected static TransferableTreeNode getTransferable() {
        return xferable;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected static boolean isValidDragSource() {
        return validDragSrc;
    }
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
	
	protected boolean allowsDropsWithinNodes, allowsDropsBetweenNodes;
    protected TreeView view;
    protected transient boolean dragSrc, dropTarget;
    protected boolean willDropAboveTarget, willDropBelowTarget, willDropOnTarget;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected AbstractTreeNodeDragAndDropController(final boolean allowsDropsWithinNodes, final boolean allowsDropsBetweenNodes) {
        constructAbstractTreeNodeDragAndDropController(allowsDropsWithinNodes, allowsDropsBetweenNodes);
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected boolean addNode() {
        debug("addNode");
        if (getActualDropAction() == DnDConstants.ACTION_MOVE) {
	        return transferNode();
        }
        return false;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public boolean allowsDropsBetweenNodes() {
        return allowsDropsBetweenNodes;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public boolean allowsDropsWithinNodes() {
        return allowsDropsWithinNodes;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected void constructAbstractTreeNodeDragAndDropController(final boolean allowsDropsWithinNodes,
    															  final boolean allowsDropsBetweenNodes) {
        this.allowsDropsWithinNodes = allowsDropsWithinNodes;
        this.allowsDropsBetweenNodes = allowsDropsBetweenNodes;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void dragDropEnd(final DragSourceDropEvent event) {
        debug("dragDropEnd");
        dragParent = dropParent = null;
        validDragSrc = validDropTarget = false;
        dragSrc = dropTarget = false;
        super.dragDropEnd(event);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Accepts TreeNode move & copy operations.
     * @since 2.1
     */
    public void dragEnter(final DropTargetDragEvent event) {
        debug("dragEnter(target)");
        super.dragEnter(event);
        validDragSrc = getValidDragSource(event.getCurrentDataFlavors());
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void dragExit(final DragSourceEvent event) {
        debug("dragExit(source)");
        event.getDragSourceContext().setCursor(DragSource.DefaultMoveNoDrop);	// Assuming move and copy cursor are identical
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void dragExit(final DropTargetEvent event) {
        debug("dragExit(target)");
        super.dragExit(event);
        validDropTarget = false;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the cursor to reflect whether the node under the mouse is a legal drop target.
    @since 2.1
    */
    public void dragOver(final DragSourceDragEvent event) {
        debug("dragOver(source)");
        // Set cursor to reflect whether drop target is legal
        final int action = getActualDropAction();
        if (validDropTarget) {
	        if (action == DnDConstants.ACTION_COPY) {
	            event.getDragSourceContext().setCursor(DragSource.DefaultCopyDrop);
	        } else {
	            event.getDragSourceContext().setCursor(DragSource.DefaultMoveDrop);
	        }
        } else {
	        if (action == DnDConstants.ACTION_COPY) {
	            event.getDragSourceContext().setCursor(DragSource.DefaultCopyNoDrop);
	        } else {
	            event.getDragSourceContext().setCursor(DragSource.DefaultMoveNoDrop);
	        }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Resets the will-drop-xxx-target and is-valid-drop-target indicators to false, resets the drop target to null.
    @param event Not used
    @since 2.0
    */
    public void dragOver(final DropTargetDragEvent event) {
        debug("dragOver(target)");
        willDropAboveTarget = willDropBelowTarget = willDropOnTarget = false;
        setDropTarget(null);
        validDropTarget = false;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Moves or copies the dragged node to the drop location determined in the dragOver(DropTargetDragEvent) method.
     * @since 2.1
     */
    public void drop(final DropTargetDropEvent event) {
        debug("drop");
        dropTarget = true;
        if (getDropTarget() != null) {
            if (dragSrc) {
                if (moveNode()) {
        			debug("drop moveNode()");
                    event.acceptDrop(event.getDropAction());
                } else {
        			debug("drop moveNode() rejectDrop()");
                    event.rejectDrop();
                }
            } else if (addNode()) {
        		debug("drop addNode()");
                event.acceptDrop(event.getDropAction());
            } else {
        		debug("drop addNode() rejectDrop()");
                event.rejectDrop();
            }
        } else {
            event.rejectDrop();
        }
        event.getDropTargetContext().dropComplete(true);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the default allowed drag actions.
     * @since 2.1
     */
    protected int getDefaultAllowedDragActions() {
		return DnDConstants.ACTION_MOVE;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the default allowed drop actions.
     * @since 2.1
     */
    protected int getDefaultAllowedDropActions() {
		return DnDConstants.ACTION_MOVE;
    }
        
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected TreeView getTreeView() {
        return view;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected boolean getValidDragSource(final DataFlavor[] flavors) {
        DataFlavor flavor;
        for (int ndx = flavors.length;  --ndx >= 0;) {
            flavor = flavors[ndx];
            if (isTreeNodeFlavor(flavor)) {
                return true;
            }
        }
        return false;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Retrieves whether the current component beneath the cursor is a valid drop target.
     * @return True if the drop target is valid
     * @since 2.1
     */
    protected boolean getValidDropTarget() {
        return validDropTarget;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected boolean isDragSource() {
        return dragSrc;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected boolean isDropTarget() {
        return dropTarget;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected boolean isTreeNodeFlavor(final DataFlavor flavor) {
        for (int ndx = TransferableTreeNode.FLAVORS.length;  --ndx >= 0;) {
            if (flavor.equals(TransferableTreeNode.FLAVORS[ndx])) {
                return true;
            }
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected boolean moveNode() {
        debug("moveNode");
        return transferNode();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setAllowsDropsBetweenNodes(final boolean allowsDropsBetweenNodes) {
        debug("setAllowsDropsBetweenNodes: " + allowsDropsBetweenNodes);
        this.allowsDropsBetweenNodes = allowsDropsBetweenNodes;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setAllowsDropsWithinNodes(final boolean allowsDropsWithinNodes) {
        debug("setAllowsDropsWithinNodes: " + allowsDropsWithinNodes);
        this.allowsDropsWithinNodes = allowsDropsWithinNodes;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected void setDropNode(final TreeNode dropNode, final boolean betweenNodes) {
        final TreeNode dragNode = (TreeNode)getDragSource();
        // Drop if:
        // - Drops within nodes are allowed
        // - The drop node allows children
        // - The drop node allows the drag node as a child
        // - The drop node is not equal to the drag node, the drag node's parent, or any of the drag node's descendents
        // - The drop node is not read-only
        if (!betweenNodes 
                && allowsDropsWithinNodes
                && view.allowsChildren(dropNode)
                && view.allowsChild(dropNode, dragNode)
                && dropNode != dragNode 
                && dropNode != view.getParent(dragNode)  
                && !view.isAncestorOf(dragNode, dropNode)
                && !view.getTreeNodeEditor().isReadOnly(dropNode)
            ) {

            setDropTarget(dropNode);
            dropParent = dropNode;
            final List children = view.getChildren(dropNode);
            if (children == null) {
                dropNdx = 0;
            } else {
                dropNdx = children.size();
            }
            validDropTarget = true;
            return;
        }
        // Drop if:
        // - Drops between nodes are allowed
        // - The drop node's parent allows the drag node as a child
        // - The drop node's parent is not equal to the drag node or any of the drag node's descendents
        // - The drop node's parent is not read-only
        // - The drop node is not equal to the drag node
        // - The drop will occur below the drop node or the drop node is not equal to the drag node's next sibling (child of drag
        //	 node's parent at drag index + 1)
        // - The drop will occur above the drop node or the drop node is not equal to the drag node's previous sibling (child of
        //	 drag node's parent at drag index - 1)
        if (betweenNodes  &&  allowsDropsBetweenNodes) {
            final TreeNode parent = view.getParent(dropNode);
            if (parent != null  &&  view.allowsChild(parent, dragNode)  &&  parent != dragNode
            	&&  !view.isAncestorOf(dragNode, parent)  &&  !view.getTreeNodeEditor().isReadOnly(parent)
            	&&  dropNode != dragNode) {

	            final List children = view.getChildren(parent);
            	if (parent != view.getParent(dragNode)
            		||  ((willDropBelowTarget  ||  children.indexOf(dropNode) != children.indexOf(dragNode) + 1)
            			 &&  (willDropAboveTarget  ||  children.indexOf(dropNode) != children.indexOf(dragNode) - 1))) {
	                setDropTarget(dropNode);
	                dropParent = parent;
	                dropNdx = view.getChildren(parent).indexOf(dropNode);
	                if (willDropBelowTarget) {
	                    ++dropNdx;
	                }
	                validDropTarget = true;
	                return;
            	}
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected void setTreeView(final TreeView view) {
        this.view = view;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether a drop at the current mouse position will occur above the current drop target.
    @param aboveTarget True if the drop will occur above the current drop target.
    @since 2.1
    */
    public void setWillDropAboveTarget(final boolean aboveTarget) {
        willDropAboveTarget = aboveTarget;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether a drop at the current mouse position will occur below the current drop target.
    @param belowTarget True if the drop will occur below the current drop target.
    @since 2.1
    */
    public void setWillDropBelowTarget(final boolean belowTarget) {
        willDropBelowTarget = belowTarget;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether a drop at the current mouse position will occur on the current drop target.
    @param onTarget True if the drop will occur on the current drop target.
    @since 2.1
    */
    public void setWillDropOnTarget(final boolean onTarget) {
        willDropOnTarget = onTarget;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Sets whether the current component beneath the cursor is a valid drop target.
     * @param valid True if the drop target is valid
     * @since 2.1
     */
    protected void setValidDropTarget(final boolean valid) {
        validDropTarget = valid;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Begins drag operations, saving reference to node being dragged.
    @since 2.1
    */
    protected void startDrag(final DragGestureEvent event, final TreeNode dragNode) {
        debug("startDrag TreeNode: " + dragNode);
        Assertion.isNotNull(view, "The setTreeView(TreeView) method must first be called with an instance of " + TreeView.class);

		ArrayList listOfOne = new ArrayList();
		listOfOne.add(dragNode);

        if (isDragCandidate(listOfOne)){
            dragSrc = true;
            setDragSource(dragNode);
            xferable = new TransferableTreeNode(listOfOne);
            dragParent = view.getParent(dragNode);
            dragNdx = view.getChildren(dragParent).indexOf(dragNode);
            debug("startDrag: dragParent=" + dragParent + ", dragNdx=" + dragNdx);
            if (event.getDragAction() == DnDConstants.ACTION_COPY) {
            	DragSource.getDefaultDragSource().startDrag(event, DragSource.DefaultCopyDrop, xferable, this);
            } else {
            	DragSource.getDefaultDragSource().startDrag(event, DragSource.DefaultMoveDrop, xferable, this);
            }
            super.dragGestureRecognized(event);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Begins drag operations, saving reference to node being dragged.
    @since 2.1
    */
//    protected void startDrag(final DragGestureEvent event, final TreeNode dragNode) {
    protected void startDrag(final DragGestureEvent event, final List dragNode) {
        debug("startDrag List: " + dragNode);
        Assertion.isNotNull(view, "The setTreeView(TreeView) method must first be called with an instance of " + TreeView.class);

        if (isDragCandidate(dragNode)){
            dragSrc = true;
            setDragSource(dragNode);
            xferable = new TransferableTreeNode(dragNode);
            dragParent = view.getParent((TreeNode)dragNode.get(0));
            dragNdx = view.getChildren(dragParent).indexOf(dragNode);
            debug("startDrag: dragParent=" + dragParent + ", dragNdx=" + dragNdx);
            if (event.getDragAction() == DnDConstants.ACTION_COPY) {
            	DragSource.getDefaultDragSource().startDrag(event, DragSource.DefaultCopyDrop, xferable, this);
            } else {
            	DragSource.getDefaultDragSource().startDrag(event, DragSource.DefaultMoveDrop, xferable, this);
            }
            super.dragGestureRecognized(event);
        }
    }
    
//    protected boolean isDragCandidate(TreeNode dragNode) {
    protected boolean isDragCandidate(List dragNode) {
//        return (dragNode != null &&  !view.getTreeNodeEditor().isReadOnly(view.getParent(dragNode)));
        final TreeNode parent = view.getParent((TreeNode)dragNode.get(0));
        return (dragNode != null  &&  parent != null  &&  !view.getTreeNodeEditor().isReadOnly(parent));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected boolean transferNode() {
        debug("transferNode");
        final TreeNodeEditor editor = view.getTreeNodeEditor();
        boolean allSuccessful = false;
        
        if ( getDragSource() instanceof TreeNode ) {

	        final UserTransaction xaction = editor.createWriteTransaction(this);
	        boolean wasErr = true;
	        try {
	            xaction.begin();
	            allSuccessful = editor.move((TreeNode)getDragSource(), dropParent, dropNdx);
	            xaction.commit();
	            wasErr = false;
	        } catch (final TransactionException err) {
	            throw new RuntimeException(err.getMessage());
	        } finally {
	            try {
	                if (wasErr) {
	                    xaction.rollback();
	                }
	            } catch (final TransactionException err) {
	                throw new RuntimeException(err.getMessage());
	            }
            }

        } else if ( getDragSource() instanceof List ) {
	
	        Iterator dragListIter = ((List)getDragSource()).iterator();
	        
	        while (dragListIter.hasNext()) {
	        	TreeNode node = (TreeNode) dragListIter.next();
	        
		        final UserTransaction xaction = editor.createWriteTransaction(this);
                allSuccessful = true;
		        boolean wasSuccessful = false;
		        boolean wasErr = true;
		        try {
		            xaction.begin();
		            wasSuccessful = editor.move(node, dropParent, dropNdx++);
		            if(!wasSuccessful) {
		            	allSuccessful = false;
		            }
		            xaction.commit();
		            wasErr = false;
		        } catch (final TransactionException err) {
		            throw new RuntimeException(err.getMessage());
		        } finally {
		            try {
		                if (wasErr) {
		                    xaction.rollback();
		                }
		            } catch (final TransactionException err) {
		                throw new RuntimeException(err.getMessage());
		            }
		        }
	        
	        
	        }
        }
        
        return allSuccessful;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether a drop at the current mouse position will occur above the current drop target.
    @return True if the drop will occur above the current drop target.
    @since 2.1
    */
    public boolean willDropAboveTarget() {
        return willDropAboveTarget;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether a drop at the current mouse position will occur below the current drop target.
    @return True if the drop will occur below the current drop target.
    @since 2.1
    */
    public boolean willDropBelowTarget() {
        return willDropBelowTarget;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether a drop at the current mouse position will occur on the current drop target.
    @return True if the drop will occur on the current drop target.
    @since 2.1
    */
    public boolean willDropOnTarget() {
        return willDropOnTarget;
    }
}
