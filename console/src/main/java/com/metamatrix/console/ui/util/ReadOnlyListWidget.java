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

import java.awt.*;
import java.awt.Color;

import javax.swing.*;
import javax.swing.ListModel;

import com.metamatrix.toolbox.ui.widget.ListWidget;

/**
 * ListWidget whose renderer will not show any list element has having focus
 * or being selected.  In other words, will make it appear as if read-only.
 */
public class ReadOnlyListWidget extends ListWidget {
	public ReadOnlyListWidget(Color foreground, Color background) {
		super();
		setRendererAndBackground(foreground, background);
	}
	
	public ReadOnlyListWidget(final java.util.List data, Color foreground,
			Color background) {
		super(data);
		setRendererAndBackground(foreground, background);
	}
	
	public ReadOnlyListWidget(final ListModel listModel, Color foreground,
			Color background) {
		super(listModel);
		setRendererAndBackground(foreground, background);
	}
	
	private void setRendererAndBackground(Color foreground, Color background) {
		this.setCellRenderer(new Renderer(foreground, background));
		this.setBackground(background);
	}
}//end ReadOnlyListWidget




class Renderer extends DefaultListCellRenderer {
	private Color foreground;
	private Color background;
	
	public Renderer(Color fg, Color bg) {
		super();
		this.foreground = fg;
		this.background = bg;
	}
	
	public Component getListCellRendererComponent(JList list, Object value,
			int index, boolean isSelected, boolean hasFocus) {
		Component comp = super.getListCellRendererComponent(list, value,
				index, false, false);
		comp.setForeground(foreground);
		comp.setBackground(background);
		return comp;
	}
}//end Renderer
			
	

