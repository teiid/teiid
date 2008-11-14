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

import java.awt.Component;
import java.awt.dnd.DnDConstants;
import java.awt.dnd.DragGestureEvent;
import java.awt.dnd.DragGestureListener;
import java.awt.dnd.DragGestureRecognizer;
import java.awt.dnd.DragSource;
import java.awt.dnd.DragSourceDragEvent;
import java.awt.dnd.DragSourceDropEvent;
import java.awt.dnd.DragSourceEvent;
import java.awt.dnd.DragSourceListener;
import java.awt.dnd.DropTarget;
import java.awt.dnd.DropTargetDragEvent;
import java.awt.dnd.DropTargetDropEvent;
import java.awt.dnd.DropTargetEvent;
import java.awt.dnd.DropTargetListener;
import java.util.Iterator;

/**
@since 2.1
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public abstract class AbstractDragAndDropController
implements DragAndDropController, DragGestureListener, DragSourceListener, DropTargetListener {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################
    
    private static final int DRAG_THRESHOLD = 3;
	private static transient Object dragSrc, dropTarget;
    private static transient boolean dragging;
    private static transient int actDropAction;
    
    private static boolean debug;
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private Component comp;
    private int allowedDragActions, allowedDropActions;
    private DragGestureRecognizer dragGestureRecognizer;
    private DropTarget dropTargetMgr;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected AbstractDragAndDropController() {
        constructAbstractDragAndDropController();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected void constructAbstractDragAndDropController() {
        dragGestureRecognizer
        	= DragSource.getDefaultDragSource().createDefaultDragGestureRecognizer(null, DnDConstants.ACTION_NONE, this);
        dropTargetMgr = new DropTarget(null, DnDConstants.ACTION_NONE, this);
        setAllowedDragActions(getDefaultAllowedDragActions());
        setAllowedDropActions(getDefaultAllowedDropActions());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
	protected void debug(final String message) {
	    if (!debug) {
	        return;
	    }
		final String name = getClass().getName();
        System.out.println(name.substring(name.lastIndexOf('.') + 1) + ": " + message);
	}
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets drag-in-progress indicator to false.
    @param event Not used
    @since 2.1
    */
	public void dragDropEnd(final DragSourceDropEvent event) {
        debug("dragDropEnd (No impl)");
        dragging = false;
        dragSrc = dropTarget = null;
        comp.repaint();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
	public void dragEnter(final DragSourceDragEvent event) {
        debug("dragEnter(source) (No impl)");
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Implemented to determine the actual drop action applicable to the drop target (assuming the drag source is valid).
     * @since 2.1
     */
	public void dragEnter(final DropTargetDragEvent event) {
        debug("dragEnter(target)");
        actDropAction = event.getDropAction();
        final int allowedActions = getAllowedDropActions();
        if ((actDropAction & allowedActions) == 0) {
            if ((allowedActions & DnDConstants.ACTION_MOVE) != 0) {
	            actDropAction = DnDConstants.ACTION_MOVE;
            } else {
	            actDropAction = DnDConstants.ACTION_COPY;
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
	public void dragExit(final DragSourceEvent event) {
        debug("dragEnter(source) (No impl)");
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
    public void dragExit(final DropTargetEvent event) {
        debug("dragEnter(target)");
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets drag-in-progress indicator to true.
    @param event Not used
    @since 2.1
    */
    public void dragGestureRecognized(final DragGestureEvent event) {
        debug("dragGestureRecognized");
        // my attempt to reduce the sensitivity of the drag gesture
        int count = 0;
        for ( Iterator i = event.iterator() ; count < DRAG_THRESHOLD && i.hasNext() ; i.next() ) {
            ++count;
        }        
        dragging = ( count >= DRAG_THRESHOLD );
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
    public void dragOver(final DragSourceDragEvent event) {
        debug("dragOver(source) (No impl)");
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
    public void dragOver(final DropTargetDragEvent event) {
        debug("dragOver(target) (No impl)");
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
    public void drop(final DropTargetDropEvent event) {
        debug("drop (No impl)");
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
    public void dropActionChanged(final DragSourceDragEvent event) {
        debug("dropActionChanged(source) (No impl)");
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Does nothing.
    @since 2.1
    */
    public void dropActionChanged(final DropTargetDragEvent event) {
        debug("dropActionChanged(target) (No impl)");
    }

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected int getActualDropAction() {
        return actDropAction;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the allowed drag actions.
     * @return The allowed drag actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    public int getAllowedDragActions() {
        return allowedDragActions;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the allowed drop actions.
     * @return The allowed drop actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    public int getAllowedDropActions() {
        return allowedDropActions;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public Component getComponent() {
        return comp;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the default allowed drag actions.
     * @since 2.1
     */
    protected int getDefaultAllowedDragActions() {
		return DnDConstants.ACTION_COPY_OR_MOVE;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the default allowed drop actions.
     * @since 2.1
     */
    protected int getDefaultAllowedDropActions() {
		return DnDConstants.ACTION_COPY_OR_MOVE;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public Object getDragSource() {
        return dragSrc;
    }
        
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public Object getDropTarget() {
        return dropTarget;
    }
        
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether a drag is in progress.
    @return True if a drag is in progress
    @since 2.1
    */
    public boolean isDragging() {
        return dragging;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Sets the allowed drag actions.
     * @param actions The allowed drag actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    public void setAllowedDragActions(int actions) {
        allowedDragActions = actions;
        dragGestureRecognizer.setSourceActions(actions);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Sets the allowed drop actions.
     * @param actions The allowed drop actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    public void setAllowedDropActions(int actions) {
        allowedDropActions = actions;
        dropTargetMgr.setDefaultActions(actions);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the Component upon which drag and drop operations can occur.
    @param component An instance of Component
    @since 2.1
    */
    public void setComponent(Component component) {
        comp = component;
        dragGestureRecognizer.setComponent(component);
        dropTargetMgr.setComponent(component);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setDragSource(final Object source) {
        dragSrc = source;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setDropTarget(final Object target) {
        dropTarget = target;
    }
}
