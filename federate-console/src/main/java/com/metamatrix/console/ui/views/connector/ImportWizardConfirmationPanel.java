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

package com.metamatrix.console.ui.views.connector;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Iterator;
import java.util.TreeMap;

import javax.swing.JPanel;

import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;



public class ImportWizardConfirmationPanel extends BasicWizardSubpanelContainer {

    private int callType;
    private JPanel pnlOuter;
    private String lblText;
    private String stepText;
    
    private int stepNumber = 4;
    
    private TreeMap connectorTypes;
    private ExtensionModule[] extensionModules;
    

    public ImportWizardConfirmationPanel(ImportWizardController cntrlr,
            WizardInterface wizardInterface, int callType) {
        super(wizardInterface);
        this.callType = callType;
        init();
    }

    private void init() {
    	if (callType == ImportWizardController.CALLED_FOR_CONNECTOR_TYPE) {
    		lblText = "New Connector Type(s):"; //$NON-NLS-1$
    	} else {
    		lblText = "New Connector Binding:"; //$NON-NLS-1$
    	}
    	
        pnlOuter = new JPanel();
        pnlOuter.setLayout(new GridBagLayout());

        
        setMainContent(pnlOuter);
        
        if (callType == ImportWizardController.CALLED_FOR_CONNECTOR_TYPE) {
        	stepText = "Click \"Finish\" to Create the new Connector Type(s)."; //$NON-NLS-1$
        } else {
        	stepText = "Click \"Finish\" to Create this new Connector Binding."; //$NON-NLS-1$
        }
        
        refresh();
    }

    private void refresh() {
        pnlOuter.removeAll();
        
        int index=0;
        if (this.connectorTypes != null && !this.connectorTypes.isEmpty()) {
            for (Iterator i = this.connectorTypes.keySet().iterator(); i.hasNext();) {
                TextFieldWidget txfItemName = new TextFieldWidget((String)i.next());
                txfItemName.setEditable(false);
    
                pnlOuter.add(new LabelWidget(lblText), new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.EAST, GridBagConstraints.NONE,new Insets(1, 0, 0, 1), 5, 4));
                pnlOuter.add(txfItemName, new GridBagConstraints(1, index, 1, 1, 1.0, 0.0,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,new Insets(1, 0, 0, 1), 5, 0));
                index++;
            }
        }
        
        // show the duplicate extension types        
        if (this.extensionModules != null && this.extensionModules.length > 0) {
            // show the duplicate extension modules
            for (int i = 0; i < this.extensionModules.length; i++) {
                TextFieldWidget txfItemName = new TextFieldWidget(extensionModules[i].getFullName());
                txfItemName.setEditable(false);
    
                pnlOuter.add(new LabelWidget("Extension Module:"), new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.EAST, GridBagConstraints.NONE,new Insets(0, 0, 5, 0), 5, 4));                
                pnlOuter.add(txfItemName, new GridBagConstraints(1, index, 1, 1, 1.0, 0.0,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,new Insets(0, 0, 5, 0), 5, 4));
                index++;
            }
        }        
        
        // finally the rest of the panel
        pnlOuter.add(new JPanel(), new GridBagConstraints(0, index,GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER,1.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.BOTH,new Insets(5, 5, 5, 5), 0, 0));
        setStepText(stepNumber, stepText);
    }
    
    public void setConnectorTypes(TreeMap list) {
        this.connectorTypes = list;
    }
    
    public void setExtensionModules(ExtensionModule[] list) {
        this.extensionModules = list;
    }    
    
    public void setStepNumber(int number) {
        this.stepNumber = number;
        refresh();
    }
}
