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

package com.metamatrix.toolbox.ui.widget.button;

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics;
import java.awt.Insets;

import javax.swing.AbstractButton;
import javax.swing.ButtonModel;
import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.swing.plaf.basic.BasicGraphicsUtils;
import javax.swing.plaf.basic.BasicBorders.ButtonBorder;

/**
 * Toggling raised/lowered bevel border for ToggleButtons
 */
public class ToggleButtonBorder extends ButtonBorder {

    private static final UIDefaults table = UIManager.getLookAndFeelDefaults();

    /**
     * Default Constructor for ToggleButtonBorder.
     */
    public ToggleButtonBorder() {
        this(table.getColor("controlShadow"),
              table.getColor("controlDkShadow"),
              table.getColor("controlHighlight"),
              table.getColor("controlLtHighlight"));
    }


	/**
	 * Constructor for ToggleButtonBorder.
	 * @param shadow
	 * @param darkShadow
	 * @param highlight
	 * @param lightHighlight
	 */
	public ToggleButtonBorder(
		Color shadow,
		Color darkShadow,
		Color highlight,
		Color lightHighlight) {
		super(shadow, darkShadow, highlight, lightHighlight);
	}


    public void paintBorder(Component c, Graphics g, int x, int y, 
                                int width, int height) {

        AbstractButton b = (AbstractButton) c;
                                    
        ButtonModel m = b.getModel();
        
        if ( m.isPressed() || m.isSelected() ) {
            BasicGraphicsUtils.drawLoweredBezel(g, x, y, width, height, 
                                                 shadow, darkShadow, 
                                                 highlight, lightHighlight);
        } else {
            BasicGraphicsUtils.drawBezel(g, x, y, width, height, 
                                                 false, false,
                                                 shadow, darkShadow, 
                                                 highlight, lightHighlight);
        }
    }


	/**
	 * @see javax.swing.border.Border#getBorderInsets(java.awt.Component)
	 */
	public Insets getBorderInsets(Component c) {
		return new Insets(2,3,2,3);
	}

}
