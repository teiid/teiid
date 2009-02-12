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

/**
@since 2.1
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public interface TreeNodeDragAndDropController
extends DragAndDropController {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether a drop at the current mouse position will occur above the current drop target.
    @return True if the drop will occur above the current drop target.
    @since 2.1
    */
    boolean willDropAboveTarget();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether a drop at the current mouse position will occur below the current drop target.
    @return True if the drop will occur below the current drop target.
    @since 2.1
    */
    boolean willDropBelowTarget();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether a drop at the current mouse position will occur on the current drop target.
    @return True if the drop will occur on the current drop target.
    @since 2.1
    */
    boolean willDropOnTarget();
}
