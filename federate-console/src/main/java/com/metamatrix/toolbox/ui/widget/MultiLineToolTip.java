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

package com.metamatrix.toolbox.ui.widget;

import java.awt.Dimension;
import java.awt.FontMetrics;
import java.awt.Toolkit;

import javax.swing.JToolTip;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.laf.MultiLineToolTipUI;

/**
 * @author Dan Florian
 * @since 3.1
 * @version 1.0
 */
public class MultiLineToolTip extends JToolTip {

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private int maxWidth = 0;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Constructor for MultiLineToolTip.
     */
    public MultiLineToolTip() {
        super();
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        maxWidth = screenSize.width / UIDefaults.getInstance().getInt("MultiLineToolTip.screenWidthFactor", 2);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public FontMetrics getFontMetrics() {
        return getFontMetrics(getFont());
    }

    public int getMaxWidth() {
        return maxWidth;
    }
    
    public void updateUI() {
        setUI(MultiLineToolTipUI.createUI(this));
    }

}
