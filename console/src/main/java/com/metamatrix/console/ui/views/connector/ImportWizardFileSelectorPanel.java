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
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.io.File;

import javax.swing.JPanel;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.util.MDCPOpenStateListener;
import com.metamatrix.console.ui.util.ModifiedDirectoryChooserPanel;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;


public class ImportWizardFileSelectorPanel extends BasicWizardSubpanelContainer
  		implements ComponentListener, MDCPOpenStateListener {
  	
    private final static String CAF_FILE_DESC = 
        "Connector Archive (*.caf)"; //$NON-NLS-1$  //$NON-NLS-2$
    private final static String CDK_FILE_DESC = 
        "Connector Types (*.cdk, *.xml)"; //$NON-NLS-1$  //$NON-NLS-2$
    private final static String CONNECTOR_BINDING_FILE_DESC = 
        "Connector Bindings (*.cdk, *.xml, *.def)"; //$NON-NLS-1$ //$NON-NLS-2$
    
    private final static String[] CAF_EXTENSIONS = 
        new String[] {"caf"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    private final static String[] CDK_EXTENSIONS = 
        new String[] {"cdk", "xml"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    private final static String[] CONNECTOR_BINDING_EXTENSIONS =
        new String[] {"cdk", "xml", "def"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
    private int callType;
    private ImportWizardControllerInterface controller;
    private FileSystemView fsvFileSysView = new FileSystemView();
    private DirectoryEntry selection;
    private ModifiedDirectoryChooserPanel chooser;
    private JPanel pnlOuter = new JPanel();
    private LabelWidget lblName;
    private TextFieldWidget txfConnectorName = new TextFieldWidget();
    
    boolean fileSelectionValid = false;
    

	public ImportWizardFileSelectorPanel(ImportWizardControllerInterface cntrlr,
            WizardInterface wizardInterface, int callType) {
        super(wizardInterface);
        controller = cntrlr;
        this.callType = callType;
        init();
    }

    private void init() {
    	String lblText;
    	if (callType == ImportWizardController.CALLED_FOR_CONNECTOR_TYPE) {
    		lblText = "Connector Name:"; //$NON-NLS-1$
    		
    	} else {
    		lblText = "Connector Binding Name:"; //$NON-NLS-1$
    	}
    	lblName = new LabelWidget(lblText);
    	
        try {
            String sInitialDir = getDefaultDirectory();
            fsvFileSysView.setHome(fsvFileSysView.lookup(sInitialDir));
        } catch (Exception ex) {
            //Any exception that occurs in setting the initial view on the
            //DirectoryChooserPanel is inconsequential.  This is merely a
            //convenience to the user.
        }
        chooser = new ModifiedDirectoryChooserPanel(fsvFileSysView,
        		DirectoryChooserPanel.TYPE_OPEN, getFileFilters(), this);
		chooser.setPreferredSize(new Dimension(475, 400));
        chooser.getAcceptButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                acceptPressed();
            }
        });
		chooser.setShowAcceptButton(false);
        chooser.setAllowFolderCreation(false);
		chooser.getCancelButton().setVisible(false);

        pnlOuter.setLayout(new GridBagLayout());

		pnlOuter.add(lblName, new GridBagConstraints(0, 0, 1, 1, 
				0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE, 
				new Insets(5, 0, 5, 0), 5, 4));
        pnlOuter.add(txfConnectorName, new GridBagConstraints(1, 0, 1, 1, 
        		1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, 
        		new Insets(5, 0, 0, 1), 5, 0));

        pnlOuter.add(chooser, new GridBagConstraints(0, 2, 2, 1, 
        		1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, 
        		new Insets(25, 0, 0, 1), 5, 0));

		addComponentListener(this);

        this.setMainContent(chooser);
        this.setStepText(1, 
        		"Select the file containing the connector definition to be imported."); //$NON-NLS-1$
	}

	public void fileSelectionIsValid(boolean flag) {
        fileSelectionValid = flag;
        enableForwardButton(fileSelectionValid);
    }


    private String getDefaultDirectory() {
        
        String sDefaultDir =
            (String) UserPreferences.getInstance().getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);
     
        return sDefaultDir;
    }

    private void saveSelectedDirectoryAsDefault() {
        DirectoryEntry deResult = (DirectoryEntry) chooser.getSelectedTreeNode();
        String fileName = deResult.getNamespace();
        int index = fileName.lastIndexOf(File.separatorChar);
        String path = fileName.substring(0, index);

        try {
            UserPreferences.getInstance().setValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY, path);
            UserPreferences.getInstance().saveChanges();
        } catch (Exception ex) {
            LogManager.logError(LogContexts.CONNECTORS, ex,
                    "Error attempting to save UserPreference for " + ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY); //$NON-NLS-1$
        }
    }


    private FileSystemFilter[] getFileFilters() {
        FileSystemFilter[] filters = null;
        if (callType == ImportWizardController.CALLED_FOR_CONNECTOR_TYPE) {
            FileSystemFilter filterCAF = 
                new FileSystemFilter(fsvFileSysView, CAF_EXTENSIONS, CAF_FILE_DESC);
            FileSystemFilter filterCDK = 
                new FileSystemFilter(fsvFileSysView, CDK_EXTENSIONS, CDK_FILE_DESC);
            filters = new FileSystemFilter[] {filterCAF, filterCDK};
        } else {
            FileSystemFilter filter = 
                new FileSystemFilter(fsvFileSysView, CONNECTOR_BINDING_EXTENSIONS, CONNECTOR_BINDING_FILE_DESC);
            filters = new FileSystemFilter[] {filter};
        }
        return filters;
    }

    
    
    

    public DirectoryEntry getSelection() {
        // we are hiding the open button, then doing the 'press' operation here
        acceptPressed();
        return selection;
    }

    private void acceptPressed() {
        selection = (DirectoryEntry)chooser.getSelectedTreeNode();
        controller.importFileSelected();
        saveSelectedDirectoryAsDefault();
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
        enableForwardButton(false);
        removeComponentListener(this);        
    }

    
    
    /** 
     * Overridden to enable the forward button only if the file selection is valid.
     * @see com.metamatrix.console.ui.util.BasicWizardSubpanelContainer#resolveForwardButton()
     * @since 4.3
     */
    public void resolveForwardButton() {
        enableForwardButton(fileSelectionValid);
    }
}
