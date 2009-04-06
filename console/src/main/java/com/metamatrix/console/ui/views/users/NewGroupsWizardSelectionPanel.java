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

package com.metamatrix.console.ui.views.users;

import java.awt.Component;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListDataEvent;
import javax.swing.event.ListDataListener;

import com.metamatrix.admin.api.objects.Group;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.NoMinTextFieldWidget;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.property.TypeConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.toolbox.ui.widget.AccumulatorPanel;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.util.StringFilter;

/**
 * Panel for selecting groups
 */
public class NewGroupsWizardSelectionPanel extends BasicWizardSubpanelContainer
                                             implements TypeConstants, GroupsAccumulatorListener {

    private JPanel pnlOuter = new JPanel();

    private JComboBox cbxDomainSelection;
    private DefaultComboBoxModel cbxmdlDomainSelectionModel;
    private GroupsAccPanel groupsAccumulator;
    private boolean hasBeenPainted = false;
    private GroupsManager groupsManager;
    private TextFieldWidget txtFieldGroupFilter;
    private ButtonWidget retrieveGroupsButton;
    private List currentDomainGroups = new ArrayList();
    private List selectedGroups = new ArrayList();
    private Collection listToRemoveFromAvailable;
    
    /**
     * Constructor
     */
    public NewGroupsWizardSelectionPanel(WizardInterface wizardInterface,
                                          GroupsManager manager, Collection removeList) {
        super(wizardInterface);
        this.groupsManager = manager;
        initExistingGroupsList(removeList);
        init();
    }

    /**
     * Create and layout all of the ui components
     */
    private void init() {
    	
    	//------------------------------------
    	// Domain Label and ComboBox
    	//------------------------------------
    	// Domain Label
        LabelWidget lblDomainName = new LabelWidget("Domain :"); //$NON-NLS-1$
        setBoldFont(lblDomainName);
        // Domain ComboBox
        JComboBox domainComboBox = getComboBox();
        // Retrieve Groups Button
        retrieveGroupsButton = new ButtonWidget("Fetch"); //$NON-NLS-1$
        retrieveGroupsButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                retrieveDomainGroups();
            }
        });
        
    	//------------------------------------------------
    	// Available Groups Filter Label and TextField
    	//------------------------------------------------
        JPanel filterPanel = getFilterPanel();

        
        // --------------------------------------
        // Groups Accumulator Panel
        // --------------------------------------
        groupsAccumulator = new GroupsAccPanel(new ArrayList(),new ArrayList(),this);
        
        
        // ---------------------------------
        // Add Components to OuterPanel
        // ---------------------------------
        GridBagLayout layout = new GridBagLayout();
        pnlOuter.setLayout(layout);
        pnlOuter.add(lblDomainName);
        pnlOuter.add(domainComboBox);
        pnlOuter.add(retrieveGroupsButton);
        pnlOuter.add(filterPanel);
        
        pnlOuter.add(groupsAccumulator);
        layout.setConstraints(lblDomainName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                                                                       GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(domainComboBox, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0,
                0));
        layout.setConstraints(retrieveGroupsButton, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0,
                0));
        
        layout.setConstraints(filterPanel, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0, GridBagConstraints.WEST,
                                                                 GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        
        layout.setConstraints(groupsAccumulator, new GridBagConstraints(0, 3, 2, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                GridBagConstraints.BOTH, new Insets(15, 5, 5, 5), 0, 0));

        
        setMainContent(pnlOuter);
        setStepText(1, "Select the Groups from the available provider domains"); //$NON-NLS-1$

        populateComboBox();
        domainComboBox.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent ev) {
				domainComboBoxSelectionChanged();
			}
		});
        
        txtFieldGroupFilter.getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                handleFilterChanged();
            }
            public void insertUpdate(DocumentEvent ev) {
            	handleFilterChanged();
            }
            public void removeUpdate(DocumentEvent ev) {
            	handleFilterChanged();
            }
        });
    }
    
    /**
     * initialize the existing groupNames list from supplied collection of MetaMatrixPrincipalNames
     */
    private void initExistingGroupsList(Collection existingPrincipalNames) {
    	this.listToRemoveFromAvailable = new ArrayList();
    	if(existingPrincipalNames!=null) {
    		Iterator iter = existingPrincipalNames.iterator();
    		while(iter.hasNext()) {
    			MetaMatrixPrincipalName principalName = (MetaMatrixPrincipalName)iter.next();
    			this.listToRemoveFromAvailable.add(principalName.getName());
    		}
    	}
    }

    /**
     * get the groupsManager
     */
    private GroupsManager getGroupsManager() {
        return this.groupsManager;
    }

    /**
     * ComboBox for selection of the available domains
     */
    private JComboBox getComboBox() {
        if (cbxDomainSelection == null) {
            cbxDomainSelection = new JComboBox();
            cbxmdlDomainSelectionModel = new DefaultComboBoxModel();
            cbxDomainSelection.setModel(cbxmdlDomainSelectionModel);
        }
        return cbxDomainSelection;
    }
    
    /**
     * Populate the domain comboBox with all available domains
     */
    private void populateComboBox() {
        Collection allDomains = null;
		try {
			allDomains = getGroupsManager().getDomainNames();
		} catch (Exception e) {
			throw new MetaMatrixRuntimeException(e);
		}
		if(allDomains!=null) {
            Iterator iter = allDomains.iterator();
            while(iter.hasNext()) {
            	String domainName = (String)iter.next();
            	cbxmdlDomainSelectionModel.addElement(domainName);
            }
            if(!allDomains.isEmpty()) {
            	cbxDomainSelection.setSelectedIndex(0);
            	// Ensure Accumulator is initialized
            	domainComboBoxSelectionChanged();
            }
		}
    }
    
    /**
     * Handler for Domain ComboBox selection changes
     */
    private void domainComboBoxSelectionChanged() {
    	this.retrieveGroupsButton.setEnabled(true);
    	
    	groupsAccumulator.setAvailableValues(new ArrayList());
    	groupsAccumulator.resetValues();
    }
    
    /**
     * Handler for fetch button - retrieves All Groups for the selected domain
     */
    private void retrieveDomainGroups() {
    	// --------------------------------------------
    	// Get all Groups for the selected Domain
    	// --------------------------------------------
    	String selectedDomain = (String)cbxDomainSelection.getSelectedItem();

    	Collection<Group> allDomainGroups = getGroupsManager().getGroupsForDomain(selectedDomain);
    	    	
    	// Remove any that are in the supplied 'remove' list
    	this.currentDomainGroups = new ArrayList(allDomainGroups.size());
    	for (Group group : allDomainGroups) {
    		String currentGroup = group.getIdentifier();
    		if(this.listToRemoveFromAvailable != null && !this.listToRemoveFromAvailable.contains(currentGroup)) {
    			this.currentDomainGroups.add(currentGroup);
    		}
    	}
    	    	
    	// --------------------------------------------
    	// Get all Groups for the selected Domain
    	// --------------------------------------------
    	groupsAccumulator.setAvailableValues(getFilteredDomainGroups());
    	
    	this.retrieveGroupsButton.setEnabled(false);
    }
    
    
    /**
     * FilterPanel for filtering the available groups
     */
    private JPanel getFilterPanel() {
    	JPanel filterPanel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        filterPanel.setLayout(layout);
    	
    	//------------------------------------------------
    	// Available Groups Filter Label and TextField
    	//------------------------------------------------
    	// Domain Label
        LabelWidget lblFilter = new LabelWidget("Groups Filter :"); //$NON-NLS-1$
        setBoldFont(lblFilter);
        // Domain ComboBox
        txtFieldGroupFilter = new NoMinTextFieldWidget(20);
        txtFieldGroupFilter.setText("*"); //$NON-NLS-1$

        filterPanel.add(lblFilter);
        filterPanel.add(txtFieldGroupFilter);

        layout.setConstraints(lblFilter, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(txtFieldGroupFilter, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
               GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0,
               0));
        
        return filterPanel;
    }

    /**
     * Handler for typing changes in the filter box
     */
    private void handleFilterChanged() {
    	groupsAccumulator.setAvailableValues(getFilteredDomainGroups());    	
    }
    
    /**
     * Get the filtered groups using the filter TextField contents
     */
    private List getFilteredDomainGroups() {
    	String filterText = txtFieldGroupFilter.getText();
    	if(filterText==null || filterText.length()==0) filterText="*";
    	StringFilter filter = new StringFilter(filterText,true);
    	
    	List filteredList = new ArrayList();
    	Iterator grpIter = this.currentDomainGroups.iterator();
    	while (grpIter.hasNext()) {
    		String group = (String)grpIter.next();
    		if(group!=null && filter.includes(group)) {
    			filteredList.add(group);
    		}
    	}
    	
    	return filteredList;
    }
    
    /**
     * Handler for changes to accumulator groups selection
     */
    public void selectedGroupsChanged() {
    	selectedGroups = this.groupsAccumulator.getValues();
    	if(!selectedGroups.isEmpty()) {
    		enableForwardButton(true);
    	} else {
    		enableForwardButton(false);
    	}
    }
    
    /**
     * Get the currently selected groups
     */
    public List getSelectedGroups() {
    	List mmPrincipalNames = new ArrayList(this.selectedGroups.size());
    	Iterator iter = selectedGroups.iterator();
    	while(iter.hasNext()) {
    		String name = (String)iter.next(); 
    		MetaMatrixPrincipalName mmName = new MetaMatrixPrincipalName(name,MetaMatrixPrincipal.TYPE_GROUP);
    		mmPrincipalNames.add(mmName);
    	}
    	
    	return mmPrincipalNames;
    }
    
    private void setBoldFont(LabelWidget label) {
        Font tempFont = label.getFont();
        Font newFont = new Font(tempFont.getName(), Font.BOLD, tempFont.getSize());
        label.setFont(newFont);
    }

    public void paint(Graphics g) {
        if (!hasBeenPainted) {
            AbstractButton forwardButton = getWizardInterface().getForwardButton();
            forwardButton.setEnabled(false);
            hasBeenPainted = true;
        }
        super.paint(g);
    }
    
    /**
     * The Groups Accumulator Panel for accumulating the selected groups.
     */
    class GroupsAccPanel extends AccumulatorPanel {
        private GroupsAccumulatorListener listener;

        public GroupsAccPanel(
                java.util.List /*<String>*/ initialAvailableContexts,
                java.util.List /*<String>*/ initialSelectedContexts,
                GroupsAccumulatorListener lsnr) {

            super(initialAvailableContexts, initialSelectedContexts);
            this.listener = lsnr;
            this.setAllowsReorderingValues(false);
            this.getAcceptButton().setVisible(false);
            this.getCancelButton().setVisible(false);
            this.getResetButton().setVisible(false);
            this.remove(this.getNavigationBar());
            this.getAvailableValuesHeader().setText("Available Groups"); //$NON-NLS-1$
            this.getValuesHeader().setText("Selected Groups"); //$NON-NLS-1$
            this.addListDataListener(new ListDataListener() {
                public void intervalAdded(ListDataEvent ev) {
                    selectionsChanged();
                }
                public void intervalRemoved(ListDataEvent ev) {
                    selectionsChanged();
                }
                public void contentsChanged(ListDataEvent ev) {
                    selectionsChanged();
                }
            });
            
            GroupListCellRenderer cellRenderer = new GroupListCellRenderer();
            this.setSelectionListCellRenderer(cellRenderer);
            this.setAvailableListCellRenderer(cellRenderer);
        }

        public void selectionsChanged() {
            listener.selectedGroupsChanged();
        }
    }
    
    public class GroupListCellRenderer extends DefaultListCellRenderer {

        public GroupListCellRenderer() {
    	    super();
        }

        public Component getListCellRendererComponent(JList list, Object value,
                int index, boolean isSelected, boolean cellHasFocus) {
        	String itemName = (String)value; 
            return super.getListCellRendererComponent(list, itemName, index, isSelected, cellHasFocus);
        }
    }

}
