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

package com.metamatrix.console.ui.views.connector;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;



public class ImportWizardDuplicatesPanel extends BasicWizardSubpanelContainer {
    private final static String OVERWRITE_CONN_TYPES = ConsolePlugin.Util.getString("ImportWizardDuplicatesPanel.OverwriteConnTypes"); //$NON-NLS-1$
    private final static String OVERWRITE_EXT_JARS = ConsolePlugin.Util.getString("ImportWizardDuplicatesPanel.OverwriteExtJars"); //$NON-NLS-1$
    
    private JPanel pnlOuter;
    private int stepNumber = 3;
    private String[] duplicateConnectorTypes;
    private String[] duplicateExtensionModules;
    private CheckBox overwriteConnTypes;
    private CheckBox overwriteExtJars;
    private String errorConnectorTypeInUseText;
    
    public ImportWizardDuplicatesPanel(ImportWizardController cntrlr, WizardInterface wizardInterface) {
        super(wizardInterface);
        init();
    }

    private void init() {
        pnlOuter = new JPanel();
        pnlOuter.setLayout(new GridBagLayout());
        setMainContent(pnlOuter);
        refresh();
    }

    private void refresh() {
        pnlOuter.removeAll();

		Font defaultFont = (new LabelWidget()).getFont();
		Font boldFont = new Font(defaultFont.getName(), Font.BOLD,
				defaultFont.getSize());

		// HasDuplicates booleans
		boolean hasDuplicateConnTypes = (this.duplicateConnectorTypes != null && this.duplicateConnectorTypes.length > 0);
        boolean hasDuplicateExtJars =   (this.duplicateExtensionModules != null && this.duplicateExtensionModules.length > 0);
        
        int index=0;
        // if there are no errors and have duplicate probs, then show options
        if (!hasInUseError() && (hasDuplicateConnTypes||hasDuplicateExtJars)) {
	        // show the duplicate connector types       
	        if (hasDuplicateConnTypes) {
	        	LabelWidget connTypeLabel = new LabelWidget("Duplicate Connector Types:");
	        	connTypeLabel.setFont(boldFont);
	            pnlOuter.add(connTypeLabel, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(5, 0, 5, 0), 5, 4));                
	            index++;
	            LabelWidget label1 = new LabelWidget("Before proceeding, you must either choose to overwrite the existing server"); //$NON-NLS-1$
	            LabelWidget label2 = new LabelWidget("connector types (see option checkbox) or press 'Back' and rename them."); //$NON-NLS-1$
	            pnlOuter.add(label1, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(0, 5, 0, 0), 5, 4));                
	            index++;
	            pnlOuter.add(label2, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(0, 5, 5, 0), 5, 4));                
	            index++;
	            for (int i = 0; i < this.duplicateConnectorTypes.length; i++) {                
	                TextFieldWidget txfItemName = new TextFieldWidget(duplicateConnectorTypes[i]);
	                txfItemName.setEditable(false);
	    
	                pnlOuter.add(txfItemName, new GridBagConstraints(0, index, 1, 1, 1.0, 0.0,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,new Insets(0, 5, 5, 1), 5, 4));
	                index++;
	            }
	        }
	        
	        // show the duplicate extension types        
	        if (hasDuplicateExtJars) {
	        	LabelWidget extJarLabel = new LabelWidget("Duplicate Extension Modules:");
	        	extJarLabel.setFont(boldFont);
	            pnlOuter.add(extJarLabel, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(5, 0, 5, 0), 5, 4));                
	            index++;
	            LabelWidget label1 = new LabelWidget("Choose to overwrite the existing server extension modules with the incoming"); //$NON-NLS-1$
	            LabelWidget label2 = new LabelWidget("modules, or leave the existing server modules (see option checkbox)"); //$NON-NLS-1$
	            pnlOuter.add(label1, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(0, 5, 0, 0), 5, 4));                
	            index++;
	            pnlOuter.add(label2, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(0, 5, 5, 0), 5, 4));                
	            index++;
	            // show the duplicate extension modules
	            for (int i = 0; i < this.duplicateExtensionModules.length; i++) {
	                TextFieldWidget txfItemName = new TextFieldWidget(duplicateExtensionModules[i]);
	                txfItemName.setEditable(false);
	    
	                pnlOuter.add(txfItemName, new GridBagConstraints(0, index, 1, 1, 1.0, 0.0,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,new Insets(0, 5, 5, 0), 5, 4));
	                index++;
	            }
	        }

	        // Show Option Checkboxes
        	LabelWidget optionLabel = new LabelWidget("Options:");
        	optionLabel.setFont(boldFont);
            pnlOuter.add(optionLabel, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(5, 0, 5, 0), 5, 4));                
            index++;
        	if(hasDuplicateConnTypes) {
	            this.overwriteConnTypes = new CheckBox(OVERWRITE_CONN_TYPES);
	            pnlOuter.add(this.overwriteConnTypes, new GridBagConstraints(0, index, 1, 1, 1.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.HORIZONTAL,new Insets(0, 5, 5, 0), 5, 4));
	            index++;
        	} 
        	if(hasDuplicateExtJars) {
	            this.overwriteExtJars = new CheckBox(OVERWRITE_EXT_JARS);
	            pnlOuter.add(this.overwriteExtJars, new GridBagConstraints(0, index, 1, 1, 1.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.HORIZONTAL,new Insets(0, 5, 5, 0), 5, 4));
	            index++;
        	}
            // finally the rest of the panel
            pnlOuter.add(new JPanel(), new GridBagConstraints(0, index, GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER,1.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.BOTH,new Insets(5, 5, 5, 5), 0, 0));
             
            // listen for the check box events.
            listenToCheckboxes();
        }
        else {
            String text = "No Duplicates or Conflicts in Connector Types/Extension Modules were found."; //$NON-NLS-1$
            if (this.errorConnectorTypeInUseText != null) {
            	LabelWidget errorLabel = new LabelWidget("Unable to Proceed:");
            	errorLabel.setFont(boldFont);
                pnlOuter.add(errorLabel, new GridBagConstraints(0, index, 1, 1, 0.0, 0.0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,new Insets(5, 0, 5, 0), 5, 4));                
                index++;
                text = "Unable to proceed, since the following connector types are in use by the bindings indicated. " + //$NON-NLS-1$
                       "To resolve, you can \n(1) cancel the import and remove the binding associations, or \n(2) go to previous step and rename the incoming binding type\n"+this.errorConnectorTypeInUseText; //$NON-NLS-1$
            }

            DefaultTextFieldModel model = new DefaultTextFieldModel();
            model.setMaximumLength(4096);
            
            JPanel panel = new JPanel();
            JScrollPane scroll = new JScrollPane();
            JTextArea pane = new JTextArea(model);
            pane.setText(text);
                        
            pane.setColumns(40);
            pane.setRows(8);
            pane.setPreferredSize(new Dimension(40, 10));
            pane.setLineWrap(true);
            pane.setWrapStyleWord(true);
            pane.setEditable(false);
            
            scroll.setViewportView(pane);
            panel.add(scroll, new GridBagConstraints(0, 0, 1, 1, 0, 0, GridBagConstraints.NORTH, GridBagConstraints.BOTH, new Insets(2, 0, 2, 0), 0, 0));
                
            pnlOuter.add(panel, new GridBagConstraints(0, index, GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER,1.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.BOTH,new Insets(5, 5, 5, 5), 0, 0));            
        }

        setStepText(stepNumber, "Resolve Duplicate Connector Types and Extension Modules.");  //$NON-NLS-1$          

    }
        
    public void setStepNumber(int number) {
        this.stepNumber = number;
        refresh();
    }

    public void setDuplicateConnectorTypes(String[] list) {
        this.duplicateConnectorTypes = list;
    }

    public void setDuplicateExtensionModules(String[] list) {
        this.duplicateExtensionModules = list;
    }
    
    public List getDuplicateExtensionModules() {
    	List dupList = Collections.EMPTY_LIST;
    	if(this.duplicateExtensionModules!=null && this.duplicateExtensionModules.length>0) {
    		dupList = new ArrayList(this.duplicateExtensionModules.length);
    		for(int i=0; i<this.duplicateExtensionModules.length; i++) {
    			dupList.add(this.duplicateExtensionModules[i]);
    		}
    	}
    	return dupList;
    }
    
    public boolean isOverwriteConnTypesPressed() {
        if (this.overwriteConnTypes != null) {
            return this.overwriteConnTypes.isSelected();
        }
        return false;
    }
    
    public boolean isOverwriteExtJarsPressed() {
        if (this.overwriteExtJars != null) {
            return this.overwriteExtJars.isSelected();
        }
        return false;
    }
    
    public void setErrorText(String text) {
        this.errorConnectorTypeInUseText = text;
    }
    
    private boolean hasInUseError() {
        return (this.errorConnectorTypeInUseText != null && this.errorConnectorTypeInUseText.length() > 0);
    }
    
    public void resolveForwardButton() {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();

        // Determine if connector type conflicts are resolved
        // either has not type conflicts, or user has agreed to overwrite
        boolean hasConnTypeDuplicates = this.duplicateConnectorTypes!=null && this.duplicateConnectorTypes.length>0;
        boolean connTypesResolved = !hasConnTypeDuplicates || ( hasConnTypeDuplicates && isOverwriteConnTypesPressed() );

        // Determine whether to enable the forward button
        // User can only continue, if (1) the types are not currently in use and
        // (2) type conflicts are ok
        boolean enable = !hasInUseError() && connTypesResolved;
        
        forwardButton.setEnabled(enable); //$NON-NLS-1$
    }
    
    private void listenToCheckboxes() {        
        if (this.overwriteConnTypes != null) {
            this.overwriteConnTypes.addChangeListener(new ChangeListener() {
                public void stateChanged(ChangeEvent e) {
                    resolveForwardButton();
                }
            });
        }
        if (this.overwriteExtJars != null) {
            this.overwriteExtJars.addChangeListener(new ChangeListener() {
                public void stateChanged(ChangeEvent e) {
                    resolveForwardButton();
                }
            });
        }
    }    
}
