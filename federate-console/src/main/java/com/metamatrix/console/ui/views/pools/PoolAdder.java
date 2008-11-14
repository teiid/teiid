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

package com.metamatrix.console.ui.views.pools;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import javax.swing.AbstractButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.console.models.PoolManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

public class PoolAdder {
    private PoolManager manager;
    private JPanel currentPanel;
    private NameAndTypePanel nameAndTypePanel;
    private PropertiesPanel propertiesPanel;
    private ConfirmationPanel confirmationPanel;
    private WizardPanelDialog dialog;
    private boolean cancelled = false;
    private boolean finished = false;
    private String[] poolTypes;
    private PoolPropertiedObjectAndEditor poae;
    private PropertiedObjectPanel pop;
    private ConfigurationID nextStartUpConfigID;
    private String prevPoolType = null;
    
    public PoolAdder(PoolManager mgr, String[] poolTypes, ConfigurationID nsu) {
        super();
        manager = mgr;
        this.poolTypes = poolTypes;
        this.nextStartUpConfigID = nsu;
    }
    
    public PoolNameAndType go() {
        //1.  Display name-and-type panel
        //2.  If name already in use, display message and return to 1.
        //3.  Create ResourceDescriptor for pool name and type entered, which
        //    will allow creation of PropertiedObject and POP.
        //4.  Display POP to enter property values, initially populated with
        //    default values.  In same panel echo the pool name and type.  In
        //	  same panel, display checkboxes for which config(s) to apply to.
        //5.  Confirmation panel.  Display all info, including POP as read/only.
        PoolNameAndType pnt = null;
        AdderWizardPanel wizardPanel = new AdderWizardPanel(this);
        nameAndTypePanel = new NameAndTypePanel(wizardPanel, poolTypes);
        propertiesPanel = new PropertiesPanel(wizardPanel);
        confirmationPanel = new ConfirmationPanel(wizardPanel);
        wizardPanel.addPage(nameAndTypePanel);
        wizardPanel.addPage(propertiesPanel);
        wizardPanel.addPage(confirmationPanel);
        wizardPanel.getNextButton().setEnabled(false);
        currentPanel = nameAndTypePanel;
        wizardPanel.getCancelButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        wizardPanel.getFinishButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                if (currentPanel == confirmationPanel) {
                    finishPressed();
                }
            }
        });
        dialog = new WizardPanelDialog(this, wizardPanel);
        dialog.show();
        if (finished && (!cancelled)) {
            try {
                pnt = manager.createPool(poae, null);
            } catch (Exception ex) {
                String msg = "Failed adding connection pool.";
                LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
                ExceptionUtility.showMessage(msg, ex);
            }
        }
        nameAndTypePanel = null;
        propertiesPanel = null;
        wizardPanel = null;
        return pnt;
    }
            
	public void dialogWindowClosing() {
	    cancelled = true;
	}
	
	private void cancelPressed() {
	    dialog.cancelPressed();
	    cancelled = true;
	}
	
	private void finishPressed() {
	    dialog.finishPressed();
	    finished = true;
	}
	
	public boolean showNextPage() {
		String poolName = nameAndTypePanel.getPoolName();
	    String poolType = nameAndTypePanel.getPoolType();
	    boolean goingToNextPage = true;
	    if (currentPanel == nameAndTypePanel) {
	        boolean exists = false;
	        try {
	            exists = manager.poolExists(poolName, nextStartUpConfigID);
            } catch (Exception ex) {
         		String msg = "Error retrieving existing pools list.";
     			LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
     			ExceptionUtility.showMessage(msg, ex);
     			goingToNextPage = false;
            }
	        if (goingToNextPage) {
    	        if (exists) {
    	            StaticUtilities.displayModalDialogWithOK(
    	            		"Pool Already Exists",
    	            		"Connection pool " + poolName + " already exists.  " +
    	            		"Must select a different name.");
    	            goingToNextPage = false;
    	        } else {
    	        	boolean useSamePOP = false;
    	        	if ((pop != null) && (poae != null) && poolType.equals(
    	        			prevPoolType)) {
    	        		poae.setPoolName(poolName);
    	        		useSamePOP = true;
    	        	}
    	        	if (!useSamePOP) {    	        				
	    	            pop = null;
	    	            try {
							poae = manager.createPropertiedObjectForPool(poolName, 
									poolType, nextStartUpConfigID);
							pop = new PropertiedObjectPanel(poae.getEditor(), manager.getEncryptor());
							pop.setNameColumnHeaderWidth(0);
							pop.setPropertiedObject(poae.getPropertiedObject());
							pop.createComponent();
	    	            } catch (Exception ex) {
	    	         		String msg = "Error retrieving properties for pool type.";
	    	     			LogManager.logError(LogContexts.RESOURCE_POOLS, ex, msg);
	    	     			ExceptionUtility.showMessage(msg, ex);
	    	     			goingToNextPage = false;
	    	            }
    	            }
    	            if (goingToNextPage) {
    	            	if (!useSamePOP) {
    	            		propertiesPanel.setPOP(pop);
    	            	}
    	                currentPanel = propertiesPanel;
    	            }
    	        }
	        }
	    } else if (currentPanel == propertiesPanel) {
	        confirmationPanel.setInfo(poolName, poolType, makePOPClone(pop));
	    	currentPanel = confirmationPanel;
	    }
	    prevPoolType = poolType;
	    return goingToNextPage;
	}
	
	public void showPreviousPage() {
	    if (currentPanel == propertiesPanel) {
	        currentPanel = nameAndTypePanel;
	    } else if (currentPanel == confirmationPanel) {
	        currentPanel = propertiesPanel;
	    }
	}
	
	private PropertiedObjectPanel makePOPClone(PropertiedObjectPanel pop) {
	    PropertiedObjectEditor poe = manager.getPropertiedObjectEditorForPool(
	    		null);
	    PropertiedObjectPanel clone = new PropertiedObjectPanel(poe, manager.getEncryptor());
	    clone.setReadOnlyForced(true);
	    clone.setNameColumnHeaderWidth(0);
	    clone.setPropertiedObject(pop.getPropertiedObject());
	    clone.createComponent();
	    return clone;
	}
}//end PoolAdder




class AdderWizardPanel extends WizardInterfaceImpl {
    private PoolAdder controller;
    
    public AdderWizardPanel(PoolAdder cntrlr) {
        super();
        controller = cntrlr;
    }
    
    public void showNextPage() {
        boolean continuing = controller.showNextPage();
        if (continuing) {
            super.showNextPage();
        }
    }
    
    public void showPreviousPage() {
        controller.showPreviousPage();
        super.showPreviousPage();
    }
}//end AddedWizardPanel




class NameAndTypePanel extends BasicWizardSubpanelContainer {
    private TextFieldWidget poolNameTFW;
    private JComboBox poolTypeCB;
    private JPanel thePanel;
    
    public NameAndTypePanel(WizardInterface wizardInterface, String[] poolTypes) {
        super(wizardInterface);
        super.setStepText(1, "Select Connection Pool name and type");
    	thePanel = createPanel(poolTypes);
    	super.setMainContent(thePanel);
    }
    
    private JPanel createPanel(String[] poolTypes) {
    	poolTypeCB = new JComboBox(poolTypes);
    	poolTypeCB.setEditable(false);
    	poolTypeCB.setSelectedIndex(0);
    	poolNameTFW = new TextFieldWidget(50);
    	JPanel panel = new JPanel();
    	GridBagLayout layout = new GridBagLayout();
    	panel.setLayout(layout);
    	LabelWidget poolNameLabel = new LabelWidget("Connection Pool name:");
    	LabelWidget poolTypeLabel = new LabelWidget("Connection Pool type:");
    	panel.add(poolNameLabel);
    	panel.add(poolNameTFW);
    	panel.add(poolTypeLabel);
    	panel.add(poolTypeCB);
    	layout.setConstraints(poolNameLabel, new GridBagConstraints(0, 0, 1, 1,
    			0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
    			new Insets(10, 10, 5, 2), 0, 0));
    	layout.setConstraints(poolNameTFW, new GridBagConstraints(1, 0, 1, 1,
    			0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
    			new Insets(10, 2, 5, 10), 0, 0));
    	layout.setConstraints(poolTypeLabel, new GridBagConstraints(0, 1, 1, 1,
    			0.0, 1.0, GridBagConstraints.NORTHEAST, GridBagConstraints.NONE,
    			new Insets(10, 10, 5, 2), 0, 0));
    	layout.setConstraints(poolTypeCB, new GridBagConstraints(1, 1, 1, 1,
    			0.0, 1.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,
    			new Insets(10, 2, 5, 10), 0, 0));
    	poolNameTFW.getDocument().addDocumentListener(new DocumentListener() {
    	    public void changedUpdate(DocumentEvent ev) {
    	        nameTextChanged();
    	    }
    	    public void insertUpdate(DocumentEvent ev) {
    	        nameTextChanged();
    	    }
    	    public void removeUpdate(DocumentEvent ev) {
    	        nameTextChanged();
    	    }
    	});
    	return panel;
    }
        
	private void nameTextChanged() {
	    AbstractButton forwardButton = getWizardInterface().getForwardButton();
	    forwardButton.setEnabled((poolNameTFW.getText().trim().length() > 0));
	}
	
	public String getPoolName() {
	    String name = poolNameTFW.getText().trim();
	    return name;
	}
	
	public String getPoolType() {
	    String type = (String)poolTypeCB.getSelectedItem();
	    return type;
	}
	
	public void postRealize() {
		poolNameTFW.requestFocus();
	}
}//end NameAndTypePanel





class PropertiesPanel extends BasicWizardSubpanelContainer 
		implements PropertyChangeListener {
    private PropertiedObjectPanel thePOP;
    private JPanel popPanel;
        
    public PropertiesPanel(WizardInterface wizardInterface) {
        super(wizardInterface);
        super.setStepText(2, "Set Connection Pool property values");
        popPanel = createPOPPanel();
        JPanel overallPanel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        overallPanel.setLayout(layout);
        overallPanel.add(popPanel);
        layout.setConstraints(popPanel, new GridBagConstraints(0, 0, 1, 1,	
        		1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
        		new Insets(0, 0, 0, 0), 0, 0));
        super.setMainContent(overallPanel);
    }
    
    public void setPOP(PropertiedObjectPanel pop) {
        thePOP = pop;
        popPanel.removeAll();
        popPanel.setLayout(new GridLayout(1, 1));
        popPanel.add(thePOP);
        thePOP.addPropertyChangeListener(this);
        setForwardButton();
    }
    
    private JPanel createPOPPanel() {
        JPanel panel = new JPanel();
        panel.setBorder(new TitledBorder("Pool Properties"));
        return panel;
    }
    
    public void propertyChange(PropertyChangeEvent ev) {
    	setForwardButton();
    }
    
    private void setForwardButton() {
    	getWizardInterface().getForwardButton().setEnabled((!anyValueInvalid()));
    }
    
    private boolean anyValueInvalid() {
    	java.util.List /*<PropertyDefinition>*/ invalidDefs =
    			thePOP.getInvalidDefinitions();
    	return (invalidDefs.size() > 0);
    }
}//end PropertiesPanel




class ConfirmationPanel extends BasicWizardSubpanelContainer {
    private JPanel panel = null;
    private TextFieldWidget poolNameTFW;
    private TextFieldWidget poolTypeTFW;
    private PropertiedObjectPanel pop;
    private JPanel popContainer;
        
    public ConfirmationPanel(WizardInterface wizardInterface) {
        super(wizardInterface);
        super.setStepText(3, "Confirmation.  Press \"Finish\" to " +
        		"create the Connection Pool.");
    }
    
    public void setInfo(String poolName, String poolType,
    		PropertiedObjectPanel inputPOP) {
    	pop = inputPOP;
		if (panel == null) {
    	    panel = new JPanel();
    	    GridBagLayout layout = new GridBagLayout();
    	    panel.setLayout(layout);
    	    LabelWidget poolNameLabel = new LabelWidget("Connection Pool name:");
    	    LabelWidget poolTypeLabel = new LabelWidget("Connection Pool type:");
    	    poolNameTFW = new TextFieldWidget(50);
    	    poolTypeTFW = new TextFieldWidget(50);
    	    poolNameTFW.setEditable(false);
    	    poolTypeTFW.setEditable(false);
    	    poolNameTFW.setText(poolName);
    	    poolTypeTFW.setText(poolType);
    	    popContainer = new JPanel(new GridLayout(1, 1));
    	    popContainer.add(pop);
    	        	    
    	    JPanel nameAndType = new JPanel();
    	    GridBagLayout nl = new GridBagLayout();
    	    nameAndType.setLayout(nl);
    	    nameAndType.add(poolNameLabel);
    	    nameAndType.add(poolTypeLabel);
    	    nameAndType.add(poolNameTFW);
    	    nameAndType.add(poolTypeTFW);
    	    nl.setConstraints(poolNameLabel, new GridBagConstraints(0, 0, 1, 1,
    	    		0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
    	    		new Insets(2, 0, 2, 2), 0, 0));
    	    nl.setConstraints(poolTypeLabel, new GridBagConstraints(0, 1, 1, 1,
    	    		0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
    	    		new Insets(2, 0, 2, 2), 0, 0));
    	    nl.setConstraints(poolNameTFW, new GridBagConstraints(1, 0, 1, 1,
    	    		1.0, 0.0, GridBagConstraints.WEST, 
    	    		GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 0), 0, 0));
    	    nl.setConstraints(poolTypeTFW, new GridBagConstraints(1, 1, 1, 1,
    	    		1.0, 0.0, GridBagConstraints.WEST,
    	    		GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 0), 0, 0));
    	    panel.add(nameAndType);
    	    layout.setConstraints(nameAndType, new GridBagConstraints(0, 0, 1, 1,
    	    		0.0, 0.0, GridBagConstraints.CENTER,
    	    		GridBagConstraints.HORIZONTAL, new Insets(4, 4, 4, 4), 0, 0));
    	    		
    	    panel.add(popContainer);
    	    layout.setConstraints(popContainer, new GridBagConstraints(0, 1,
    	    		1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
    	    		GridBagConstraints.BOTH, new Insets(4, 4, 4, 4), 0, 0));
    	    		
    	    super.setMainContent(panel);
    	} else {
    	    poolNameTFW.setText(poolName);
    	    poolTypeTFW.setText(poolType);
    	    popContainer.removeAll();
    	    popContainer.add(pop);
    	}
    }
}//end ConfirmationPanel


    	    
    	    
class WizardPanelDialog extends JDialog {
    private PoolAdder caller;
    private AdderWizardPanel wizardPanel;
    
    public WizardPanelDialog(PoolAdder cllr, AdderWizardPanel wizPnl) {
        super(ConsoleMainFrame.getInstance(), "Add Connection Pool Wizard");
        caller = cllr;
        wizardPanel = wizPnl;
        this.setModal(true);
        this.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                caller.dialogWindowClosing();
            }
        });
        init();
        this.pack();
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension size = this.getSize();
        Dimension newSize = new Dimension(
        		Math.max(size.width, (int)(screenSize.width * 0.5)),
        		Math.max(size.height, (int)(screenSize.height * 0.75)));
        this.setSize(newSize);
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }
    
    private void init() {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        this.getContentPane().add(wizardPanel);
        layout.setConstraints(wizardPanel, new GridBagConstraints(0, 0, 1, 1,
        		1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
        		new Insets(0, 0, 0, 0), 0, 0));
    }
    
    public void cancelPressed() {
        this.dispose();
    }
    
    public void finishPressed() {
        this.dispose();
    }
}//end WizardPanelDialog
