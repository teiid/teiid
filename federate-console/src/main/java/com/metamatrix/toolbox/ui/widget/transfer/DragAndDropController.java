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

/**
 * Provides the base interface necessary to support both drag and drop (D&D) operations for a component.
 * @since 2.1
 * @version 2.1
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public interface DragAndDropController {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the allowed drag actions.
     * @return The allowed drag actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    int getAllowedDragActions();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the allowed drop actions.
     * @return The allowed drop actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    int getAllowedDropActions();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the Component upon which drag and drop operations can occur.
     * @return The Component upon which drag and drop operations can occur.
     * @since 2.1
     */
    Component getComponent();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Returns the object being dragged.
     * @return The object being dragged
     * @since 2.1
     */
    Object getDragSource();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Returns the object that would receive the dragged object if it were dropped at the current mouse position.
     * @return The node being dragged
     * @since 2.1
     */
    Object getDropTarget();
        
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Returns whether a drag is in progress.
     * @return True if a drag is in progress
     * @since 2.1
     */
    boolean isDragging();

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Sets the allowed drag actions.
     * @param actions The allowed drag actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    void setAllowedDragActions(int actions);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Sets the allowed drop actions.
     * @param actions The allowed drop actions
     * @see DnDConstants#ACTION_COPY
     * @see DnDConstants#ACTION_MOVE
     * @see DnDConstants#ACTION_COPY_OR_MOVE
     * @since 2.1
     */
    void setAllowedDropActions(int actions);
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Sets the Component upon which drag and drop operations can occur.
     * @param component An instance of Component
     * @since 2.1
     */
    void setComponent(Component component);
}
