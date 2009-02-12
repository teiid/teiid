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

import java.awt.Color;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.Icon;
import javax.swing.JPanel;

import com.metamatrix.toolbox.ui.widget.LabelWidget;

/**
 * Extension to JPanel consisting of an icon followed in the same row by a label.
 */
public class IconLabel extends JPanel {
    private LabelWidget label;
    private IconComponent ic;

    public IconLabel(Icon icon, String text) {
        super();
        label = new LabelWidget(text);
        ic = new IconComponent(icon);
        init();
    }

    public void setLabelColor(Color clr) {
        label.setForeground(clr);
    }

    public void setLabelFont(Font font) {
        label.setFont(font);
    }

    private void init() {
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        add(ic);
        layout.setConstraints(ic, new GridBagConstraints(0, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        add(label);
        layout.setConstraints(label, new GridBagConstraints(1, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
    }

}
