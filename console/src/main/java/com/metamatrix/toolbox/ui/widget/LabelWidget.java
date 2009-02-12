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
package com.metamatrix.toolbox.ui.widget;

// System imports
import javax.swing.Icon;
import javax.swing.JLabel;
import javax.swing.JToolTip;

import com.metamatrix.toolbox.ui.TextUtilities;
import com.metamatrix.toolbox.ui.widget.laf.LabelLookAndFeel;

/**
This class is intended to be used everywhere within the application that a label needs to be displayed.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class LabelWidget extends JLabel {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    private String textFmt; // Initialized by superclass

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a blank label.  Unlike a JLabel, it will have the same height as a label with a single line of text.
    @since 2.0
    */
    public LabelWidget() {
        this(null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a label with the specified text.
    @param text The text
    @since 2.0
    */
    public LabelWidget(final String text) {
        this(text, null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a label with the specified icon.
    @param icon The icon
    @since 2.0
    */
    public LabelWidget(final Icon icon) {
        this(null, icon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a label with the specified text and icon.  The label is by default laid out with the icon to the left of the text.
    @param text The text
    @param icon The icon
    @since 2.0
    */
    public LabelWidget(final String text, final Icon icon) {
        super(text, icon, LEFT);
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
    @return The text format
    @since 2.0
    */
    public String getTextFormat() {
        return textFmt;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setText(final String text) {
        textFmt = text;
        super.setText(TextUtilities.getUnformattedText(text));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The text without HTML formatting
    @since 2.0
    */
    public String toString() {
        return getText();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void updateUI() {
        setUI(LabelLookAndFeel.createUI(this));
    }
}
