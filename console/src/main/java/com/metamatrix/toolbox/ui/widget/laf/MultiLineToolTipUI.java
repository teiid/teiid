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

package com.metamatrix.toolbox.ui.widget.laf;

import java.awt.Dimension;
import java.awt.Graphics;

import javax.swing.CellRendererPane;
import javax.swing.JComponent;
import javax.swing.JTextArea;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.basic.BasicToolTipUI;

import com.metamatrix.toolbox.ui.widget.MultiLineToolTip;

/**
 * @author Dan Florian
 * @since 3.1
 * @version 1.0
 */
public class MultiLineToolTipUI extends BasicToolTipUI {

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private CellRendererPane rendererPane;
    private JTextArea txa;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private MultiLineToolTipUI() {
        txa = new JTextArea();
        txa.setWrapStyleWord(true);
        rendererPane = new CellRendererPane();
        rendererPane.add(txa);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public static ComponentUI createUI(JComponent theComponent) {
        return new MultiLineToolTipUI();
    }

    public Dimension getMaximumSize(JComponent theComponent) {
        return getPreferredSize(theComponent);
    }

    public Dimension getMinimumSize(JComponent theComponent) {
        return getPreferredSize(theComponent);
    }

    public Dimension getPreferredSize(JComponent theComponent) {
        Dimension result = new Dimension(0, 0);
        MultiLineToolTip toolTip = (MultiLineToolTip)theComponent;
        String tipText = toolTip.getTipText();

        if (tipText != null) {
            txa.setText(tipText);
            txa.setLineWrap(false);

            int textWidth = toolTip.getFontMetrics().stringWidth(tipText);
            int maxWidth = toolTip.getMaxWidth();

            if (textWidth > maxWidth) {
                txa.setLineWrap(true);
                Dimension d = txa.getPreferredSize();
                d.width = maxWidth;
                ++d.height;
                txa.setSize(d);
            }

            result = txa.getPreferredSize();
            ++result.height;
            ++result.width;
        }
    
        return result;
    }

    public void installUI(JComponent theComponent) {
        super.installUI(theComponent);

        theComponent.add(rendererPane);
    }

    public void paint(Graphics theGraphics,
                      JComponent theComponent) {
        Dimension size = theComponent.getSize();
        txa.setBackground(theComponent.getBackground());
        rendererPane.paintComponent(theGraphics, txa, theComponent, 1, 1, size.width, size.height, true);
    }

    public void uninstallUI(JComponent theComponent) {
        super.uninstallUI(theComponent);

        theComponent.remove(rendererPane);
    }

}
