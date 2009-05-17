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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ItemListener;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

public class ExpertPropertiedObjectPanelHolder extends JPanel {
    protected CheckBox expertPropertiesCheckBox = null;
    protected PropertiedObjectPanel thePanel;

	public ExpertPropertiedObjectPanelHolder(PropertiedObjectPanel pnl,
            ItemListener expertCheckBoxChangeListener) {
		this.thePanel = pnl;
        init(expertCheckBoxChangeListener);
    }

    protected void init(ItemListener expertCheckBoxChangeListener) {
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
            expertPropertiesCheckBox = new CheckBox("Include expert properties", thePanel.getShowExpertProperties()?CheckBox.SELECTED:CheckBox.DESELECTED); //$NON-NLS-1$
            expertPropertiesCheckBox.addItemListener(expertCheckBoxChangeListener);
            buttonPanel.add(expertPropertiesCheckBox);
        } else {
            layout.setConstraints(buttonPanel, new GridBagConstraints(0, 0, 1, 1,
                    0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        
    }

    public boolean isIncludingExpertProperties() {
        return expertPropertiesCheckBox.isSelected();
    }

    public void setIsIncludingExpertProperties( boolean b ) {
        expertPropertiesCheckBox.setSelected( b );
    }
    
    public PropertiedObjectPanel getThePanel() {
        return thePanel;
    }
}
