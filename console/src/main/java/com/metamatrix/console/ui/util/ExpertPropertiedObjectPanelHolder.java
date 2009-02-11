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

package com.metamatrix.console.ui.util;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ItemListener;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

public class ExpertPropertiedObjectPanelHolder extends AbstractPropertiedObjectPanelHolder {
    protected CheckBox expertPropertiesCheckBox = null;

	public ExpertPropertiedObjectPanelHolder(PropertiedObjectPanel pnl,
            ItemListener expertCheckBoxChangeListener, ItemListener optionalCheckBoxChangeListener) {
        super(pnl);
        init(expertCheckBoxChangeListener, optionalCheckBoxChangeListener);
    }

    protected void init(ItemListener expertCheckBoxChangeListener, ItemListener optionalCheckBoxChangeListener) {
        setBorder(BorderFactory.createEtchedBorder());
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        add(thePanel);
        layout.setConstraints(thePanel, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(2, 2, 2, 2), 0, 0));
        
        JPanel buttonPanel = new JPanel();
        add(buttonPanel);
        layout.setConstraints(buttonPanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST,
                GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        if (expertCheckBoxChangeListener != null) {
            expertPropertiesCheckBox = new CheckBox("Include expert properties"); //$NON-NLS-1$
            expertPropertiesCheckBox.addItemListener(expertCheckBoxChangeListener);
            buttonPanel.add(expertPropertiesCheckBox);
        }
        
        if (optionalCheckBoxChangeListener != null) {
            optionalPropertiesCheckBox = new CheckBox("Include optional properties", CheckBox.SELECTED); //$NON-NLS-1$
            optionalPropertiesCheckBox.addItemListener(optionalCheckBoxChangeListener);
            buttonPanel.add(optionalPropertiesCheckBox);
        }
    }

    public boolean isIncludingExpertProperties() {
        return expertPropertiesCheckBox.isSelected();
    }

    public void setIsIncludingExpertProperties( boolean b ) {
        expertPropertiesCheckBox.setSelected( b );
    }
}
