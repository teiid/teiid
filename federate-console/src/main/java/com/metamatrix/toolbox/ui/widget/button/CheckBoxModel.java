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
package com.metamatrix.toolbox.ui.widget.button;

// System imports
import java.awt.event.ItemEvent;
import javax.swing.JToggleButton;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class CheckBoxModel extends JToggleButton.ToggleButtonModel {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    public static final int PARTIALLY_SELECTED  = Math.max(ItemEvent.SELECTED, ItemEvent.DESELECTED) + 1;
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private boolean isPartiallySelected = false;
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public boolean isPartiallySelected() {
        return isPartiallySelected;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setPartiallySelected(final boolean isPartiallySelected) {
        if (this.isPartiallySelected == isPartiallySelected  &&  !isSelected()) {
            return;
        }
        this.isPartiallySelected = isPartiallySelected;
        stateMask &= ~SELECTED;
        fireItemStateChanged(new ItemEvent(this, ItemEvent.ITEM_STATE_CHANGED, this,
                                           isPartiallySelected ?  PARTIALLY_SELECTED : ItemEvent.DESELECTED));
        fireStateChanged();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setSelected(final boolean isSelected) {
        isPartiallySelected = isSelected;
        super.setSelected(isSelected);
    }
}
