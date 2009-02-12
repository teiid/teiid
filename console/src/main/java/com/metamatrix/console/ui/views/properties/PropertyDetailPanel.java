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

package com.metamatrix.console.ui.views.properties;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.border.Border;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class PropertyDetailPanel extends JPanel{
    private TextFieldWidget dispNameField, nextStartupField, identifierField;
    private JTextArea descriptionTextArea;
    private String title;
    private JScrollPane scpnDescription = new JScrollPane();

    public PropertyDetailPanel(String title) {
        super();
        this.title = title;
        creatComponent();

    }
    private void creatComponent(){
        Border border2;
        setLayout(new GridBagLayout());
        border2 = BorderFactory.createEmptyBorder(0,11,0,11);
        this.setBorder(border2);
        dispNameField = new TextFieldWidget(10);
        dispNameField.setName("SystemProperties."+title);
        dispNameField.setEditable(false);
        nextStartupField = new TextFieldWidget(10);
        nextStartupField.setName("SystemProperties."+title);
        nextStartupField.setEditable(false);
        identifierField = new TextFieldWidget(10);
        identifierField.setEditable(false);
        LabelWidget dispNameJL = new LabelWidget("Display Name: ");
        LabelWidget nextStartupJL = new LabelWidget("Next Startup Value: ");
        LabelWidget identifierJL = new LabelWidget("Identifier: ");
        identifierJL.setName("SystemProperties."+title);
        nextStartupJL.setName("SystemProperties."+title);
        dispNameJL.setLabelFor(dispNameField);
        nextStartupJL.setLabelFor(nextStartupField);
        identifierJL.setLabelFor(identifierField);
        
        LabelWidget descriptionJL = new LabelWidget("Description: ");
        descriptionTextArea = new JTextArea();
        scpnDescription.setViewportView(descriptionTextArea);
        descriptionTextArea.setName("SystemProperties."+title);
        descriptionTextArea.setLineWrap(true);
        descriptionTextArea.setWrapStyleWord(true);
        descriptionTextArea.setEditable(false);

        this.add(scpnDescription, new GridBagConstraints(1,4,1,2,0.1,0.1,
                                         GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(1, 0, 1, 0), 0, 0));
        this.add(descriptionJL, new GridBagConstraints(0,4,1,1,0,0,
                                         GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 0, 2, 0), 0, 0));
        this.add(dispNameJL, new GridBagConstraints(0,0,1,1,0,0,
                                         GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 1, 2, 1), 0, 0));
        this.add(dispNameField, new GridBagConstraints(1,0,1,1,0,0,
                                         GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(2, 1, 2, 1), 0, 0));
        this.add(nextStartupJL, new GridBagConstraints(0,2,1,1,0,0,
                                         GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 1, 2, 1), 0, 0));
        this.add(nextStartupField, new GridBagConstraints(1,2,1,1,0,0,
                                         GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(2, 1, 2, 1), 0, 0));
        this.add(identifierJL, new GridBagConstraints(0,3,1,1,0,0,
                                         GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 1, 2, 1), 0, 0));
        this.add(identifierField, new GridBagConstraints(1,3,1,1,0,0,
                                         GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(2, 1, 2, 1), 0, 0));
        
    }
    
    public void setDisplayName(String dName){
        dispNameField.setText(dName);
    }
    
    public void setIdentifierField (String identifierName){
        identifierField.setText(identifierName);
    }

    public void setDescriptionName(String descriptionName){
        descriptionTextArea.setText(descriptionName);
    }

    public void setNSUPropertyValue(String pName){
        nextStartupField.setText(pName);
    }
}
