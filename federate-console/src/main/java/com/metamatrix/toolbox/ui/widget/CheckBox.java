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
package com.metamatrix.toolbox.ui.widget;

// System imports
import java.awt.Color;

import javax.swing.ButtonModel;
import javax.swing.JCheckBox;
import javax.swing.JToolTip;

import com.metamatrix.toolbox.ui.widget.button.CheckBoxModel;
import com.metamatrix.toolbox.ui.widget.laf.CheckBoxLookAndFeel;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class CheckBox extends JCheckBox {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    public static final int DESELECTED          = 0;
    public static final int SELECTED            = 1;
    public static final int PARTIALLY_SELECTED  = 2;

    public static final String PROPERTY_PREFIX = "CheckBox.";

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private final transient int selectionState;
    private Color boxBkgdColor;
    private Color boxDisabledBkgdColor;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public CheckBox() {
        this(null, DESELECTED);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public CheckBox(final String text) {
        this(text, DESELECTED);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public CheckBox(final int selectionState) {
        this(null, selectionState);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public CheckBox(final String text, final int selectionState) {
        super(text);
        this.selectionState = selectionState;
        initializeCheckBox();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    public JToolTip createToolTip() {
        JToolTip tip = new MultiLineToolTip();
        tip.setComponent(this);
        return tip;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public Color getBoxBackgroundColor() {
        return boxBkgdColor;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public Color getBoxDisabledBackgroundColor() {
        return boxDisabledBkgdColor;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void initializeCheckBox() {
        setModel(new CheckBoxModel());
        switch (selectionState) {
            case DESELECTED: {
                break;
            }
            case SELECTED: {
                setSelected(true);
                break;
            } 
            case PARTIALLY_SELECTED: {
                setPartiallySelected(true);
                break;
            } 
            default: {
                throw new IllegalArgumentException("Invalid selection state: " + selectionState);
            }
        }
        setBorder(null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public boolean isPartiallySelected() {
        final ButtonModel model = getModel();
        if (!(model instanceof CheckBoxModel)) {
            return false;
        }
        return ((CheckBoxModel)model).isPartiallySelected();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setBoxBackgroundColor(final Color color) {
        boxBkgdColor = color;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setBoxDisabledBackgroundColor(final Color color) {
        boxDisabledBkgdColor = color;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setPartiallySelected(final boolean isPartiallySelected) {
        final ButtonModel model = getModel();
        if (!(model instanceof CheckBoxModel)) {
            return;
        }
        ((CheckBoxModel)model).setPartiallySelected(isPartiallySelected);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void updateUI() {
        setUI(CheckBoxLookAndFeel.createUI(this));
    }
}
