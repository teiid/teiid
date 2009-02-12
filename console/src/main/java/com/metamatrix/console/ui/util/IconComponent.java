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

package com.metamatrix.console.ui.util;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;

import javax.swing.Icon;
import javax.swing.JPanel;

/**
 * Extension to JComponent to represent a component consisting of one or more
 * Icons to be presented horizontally, and nothing else.  Also implements Icon,
 * so can be used to paint its Icons onto another Component.
 */
public class IconComponent extends JPanel implements Icon {
    private Icon[] icons;
    private int iconWidth;
    private int iconHeight;

	public IconComponent(Icon[] icns) {
		super();
		icons = icns;
	}
	
    public IconComponent(Icon icn) {
        super();
        icons = new Icon[] {icn};
        setSize();
    }

    private void setSize() {
    	int totalWidth = 0;
    	int maxHeight = 0;
    	for (int i = 0; i < icons.length; i++) {
    		int curWidth = icons[i].getIconWidth();
    		int curHeight = icons[i].getIconHeight();
    		totalWidth += curWidth;
    		if (curHeight > maxHeight) {
    			maxHeight = curHeight;
    		}
    	}
    	iconWidth = totalWidth;
    	iconHeight = maxHeight;
    }

    public Dimension getMinimumSize() {
    	return new Dimension(iconWidth, iconHeight);
    }

	public Dimension getPreferredSize() {
		return getMinimumSize();
	}
	    
    public void paint(Graphics g) {
    	int widthSoFar = 0;
    	for (int i = 0; i < icons.length; i++) {
    		int curHeight = icons[i].getIconHeight();
    		int curWidth = icons[i].getIconWidth();
    		int excessHeight = iconHeight - curHeight;
    		int yOffset = (excessHeight / 2);
    		icons[i].paintIcon(this, g, widthSoFar, yOffset);
    		widthSoFar += curWidth;
    	}
    }
    
    public int getIconHeight() {
    	return iconHeight;
    }
    
    public int getIconWidth() {
    	return iconWidth;
    }
    
    public void paintIcon(Component comp, Graphics g, int x, int y) {
    	int widthSoFar = 0;
    	for (int i = 0; i < icons.length; i++) {
    		int curHeight = icons[i].getIconHeight();
    		int curWidth = icons[i].getIconWidth();
    		int excessHeight = iconHeight - curHeight;
    		int yOffset = (excessHeight / 2);
    		icons[i].paintIcon(comp, g, widthSoFar, yOffset);
    		widthSoFar += curWidth;
    	}
    }
}
