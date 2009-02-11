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

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.AbstractButton;
import javax.swing.JPanel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.TypeConstants;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;


public class ImportWizardConnectorIDPanel extends BasicWizardSubpanelContainer implements ComponentListener, TypeConstants {

	private int callType;    
    private TextFieldWidget[] txfItemName;
    private boolean hasBeenPainted = false;
    private Map connectorTypesToImport;
    ExtensionModule[] extensionModulesToImport;
    JPanel pnlOuter = new JPanel();
    private DirectoryEntry prevDirEntry;
    
	public ImportWizardConnectorIDPanel(WizardInterface wizardInterface, int callType) {
        super(wizardInterface);
        this.callType = callType;
        init();
    }

    private void init() {
    	String lblText;
    	if (callType == ImportWizardController.CALLED_FOR_CONNECTOR_TYPE) {
    		lblText = "Connector Name: "; //$NON-NLS-1$
    	} else {
    		lblText = "Connector Binding:"; //$NON-NLS-1$
    	}
        
		pnlOuter.setLayout(new GridBagLayout());
        int index = 0;        
        if (this.connectorTypesToImport != null) {
            pnlOuter.removeAll();

            // show all the connector types
            txfItemName = new TextFieldWidget[this.connectorTypesToImport.keySet().size()];
            for (Iterator i = this.connectorTypesToImport.keySet().iterator(); i.hasNext();) {                
                txfItemName[index] = GuiComponentFactory.createTextField(CONNECTOR_TYPE_NAME);
                txfItemName[index].setPreferredSize(new Dimension(225, 21));
                txfItemName[index].setText((String)i.next());

                pnlOuter.add(new LabelWidget(lblText), new GridBagConstraints(0, index, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(0, 0, 5, 0), 5, 4));
                pnlOuter.add(txfItemName[index], new GridBagConstraints(1, index, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 5, 1), 5, 4));                
                index++;
            }
        }
        
        // show the extension modules
        if (this.extensionModulesToImport != null && this.extensionModulesToImport.length > 0 ) {
            for (int i = 0; i < this.extensionModulesToImport.length; i++) {

                TextFieldWidget txfItemName = new TextFieldWidget(this.extensionModulesToImport[i].getFullName());
                txfItemName.setEditable(false);
                
                pnlOuter.add(new LabelWidget("Extension Module:"), new GridBagConstraints(0, index, 1, 1, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(0, 0, 5, 0), 5, 4)); //$NON-NLS-1$
                pnlOuter.add(txfItemName, new GridBagConstraints(1, index, 1, 1, 0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 5, 1), 5, 4));
                index++;
            }
        }
        
        // add a filler panel
		pnlOuter.add(new JPanel(), new GridBagConstraints(0, index, 
				GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER, 
				1.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.BOTH, 
				new Insets(5, 5, 5, 5), 0, 0));

        setMainContent(pnlOuter);
        String stepText;
        if (callType == ImportWizardController.CALLED_FOR_CONNECTOR_TYPE) {
        	stepText = "Specify or Modify a Name for this Connector(s)."; //$NON-NLS-1$
        } else {
        	stepText = "Specify a Name for this Connector Binding."; //$NON-NLS-1$
        }
        setStepText(2, stepText);
    }

    public void setupListening() {

        DocumentListener listener = new DocumentListener() {
            public void changedUpdate(DocumentEvent de) {
                resolveForwardButton();
            }
            public void insertUpdate(DocumentEvent de) {
                resolveForwardButton();
            }
            public void removeUpdate(DocumentEvent de) {
                resolveForwardButton();
            }
        };
        
        for (int i = 0; i < txfItemName.length; i++) {
            txfItemName[i].getDocument().addDocumentListener(listener);
        }
        
        addComponentListener(this);
    }

	public void resolveForwardButton() {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        
        // if any one of the connectors is selected then enable the button.
        boolean enable = false;
        for (int i = 0; i < this.txfItemName.length; i++) {
            if (!txfItemName[i].getText().trim().equals("")) {
                enable = true;
            }
        }
        
        forwardButton.setEnabled(enable); //$NON-NLS-1$
    }

    // methods required by ComponentListener Interface

    public void componentMoved(ComponentEvent e) {
        setInitialPostRealizeState();
    }

    public void componentResized(ComponentEvent e) {
        // This is John V's recommendation for disabling the Next button
        // on the first appearance of the first page:
        setInitialPostRealizeState();
    }

    public void componentShown(ComponentEvent e) {
        setInitialPostRealizeState();
    }

    public void componentHidden(ComponentEvent e) {
        setInitialPostRealizeState();
    }

    public void setInitialPostRealizeState() {
        // May no longer need this, since we are supplying a default name:
        //   enableNextButton(false);
        removeComponentListener(this);
    }
    
    public void paint(Graphics g) {
    	super.paint(g);
    	if (!hasBeenPainted) {
    		hasBeenPainted = true;
    		txfItemName[0].requestFocus();
    	}
    }


    public void setConnectorTypes(DirectoryEntry dirEntry, Map connectorTypes) {
        boolean build = true;
        
        // we only need to do this first time so that back/next button will not messup the text
        if (this.connectorTypesToImport == null) {
            prevDirEntry = dirEntry;
            
        }
        else {
            try {
                build = !dirEntry.toURL().equals(this.prevDirEntry.toURL());
            } catch (MalformedURLException e) {
                // ignore
            }
        }
                
        // we only need to do this first time so that back/next button will not messup the text
        if (build) {
            prevDirEntry = dirEntry;
            this.connectorTypesToImport = connectorTypes;
            init();
            setupListening();
            resolveForwardButton();
        }
    }

    public void setExtensionModules(ExtensionModule[] modules) {
        this.extensionModulesToImport = modules;
    }

    public TreeMap getConnectorTypes() {
        TreeMap map = new TreeMap();
        
        int index = 0;        
        for (Iterator i = this.connectorTypesToImport.keySet().iterator(); i.hasNext();) {
            String newCTypeName= txfItemName[index].getText();
            if (newCTypeName != null) {
                newCTypeName = newCTypeName.trim();
            }
            
            ConnectorBindingType cType = (ConnectorBindingType)this.connectorTypesToImport.get(i.next());
            
            if (newCTypeName != null && newCTypeName.length() > 0) {
                map.put(newCTypeName, cType);
            }
            
            index++;
        }
        return map;
    }

}
