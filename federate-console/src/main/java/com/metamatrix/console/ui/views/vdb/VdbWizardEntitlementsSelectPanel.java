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

package com.metamatrix.console.ui.views.vdb;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;


/*
 * Panel for selection of Roles Import or Migration options
 */
public class VdbWizardEntitlementsSelectPanel extends BasicWizardSubpanelContainer {
	private JPanel pnlOuter = new JPanel();
	private ButtonWidget btnBack = new ButtonWidget();
	private JPanel pnlBlather = new JPanel();
	private BorderLayout borderLayout1 = new BorderLayout();
	private BorderLayout borderLayout2 = new BorderLayout();
	private BorderLayout borderLayout4 = new BorderLayout();
	private JPanel pnlWorkSpace = new JPanel();
	private ButtonWidget btnCancel = new ButtonWidget();
	private JLabel lblBlather = new JLabel();
	private JPanel pnlButtons = new JPanel();
	private GridLayout gridLayout1 = new GridLayout();
	private JPanel pnlRealButtons = new JPanel();
	private Border border1;
	private Border border3;
	private TextFieldWidget txvVersion = new TextFieldWidget();
	private JLabel lblVersion = new JLabel();
	private TextFieldWidget txfName = new TextFieldWidget();
	private JLabel lblName = new JLabel();
	private GridBagLayout gridBagLayout1 = new GridBagLayout();
	private JPanel pnlWorkspaceOuter = new JPanel();
	private BorderLayout borderLayout3 = new BorderLayout();
	private JTextArea txaConfirmationMessage = new JTextArea();
	private Border border5;
	private TextFieldWidget txfStatus = new TextFieldWidget();
	private JLabel lblStatus = new JLabel();
	private Border border6;

    // data
    private String sVdbName         = "";
    private short siStatus          = 0;

    // Entitlements widgets
    private AbstractButton radioNoRolesImport = new JRadioButton( " Do not Import or Migrate Roles");
    private AbstractButton radioImportVdbRoles = new JRadioButton( " Import Role Definitions contained in the VDB");
    private AbstractButton radioMigrateVdbRoles = new JRadioButton( " Migrate Roles from the previous VDB version");
    private CheckBox cbxViewVdbRolesReport = new CheckBox( " View Incoming Roles Report",CheckBox.DESELECTED);

    // VDB Status messages
    private String INCOMPLETE_VDB_MESSAGE
        =   "Please note: Since Connector Bindings were not specified for all " +
            "of the models, the VDB will have an initial status of Incomplete, " +
            "until you specify all Bindings.  ";

    private String INACTIVE_VDB_MESSAGE
        =   "Please note: Connector Bindings have been specified for all " +
            "of the models.  The VDB will have an initial status of Inactive.  ";
    
    private String ROLE_IMPORT_MESSAGE
		=   "\n\nPlease select the desired role import option.  \n\nIf roles are " +
		    "used at your installation, they must be defined for a VDB before applications can use it.";

    private String ROLE_IMPORT_OR_MIGRATE_MESSAGE
		=   "\n\nPlease select the desired role import or migration option.  \n\nIf roles are " +
		    "used at your installation, they must be defined for a VDB before applications can use it.";
    
    private String NO_ROLES_AVAILABLE_TEXT =  "No roles are available for import.";    

    // used when this panel is doing a new version
    VirtualDatabase vdbSourceVdb        = null;

    boolean showMigrationOption         = false;
    boolean roleImportOnly				 = false;
    boolean showVdbRolesImportOption    = false;

    /*
     * Constructor.  This constructor is used when a new vdb is being imported created 
     * (not versioning an existing vdb)
     * @param wizardInterface the wizard interface
     * @param stepNum the step number for this wizard panel
     */
    public VdbWizardEntitlementsSelectPanel(WizardInterface wizardInterface, int stepNum) {
        super(wizardInterface);
        // new VDB, do not show the role migration option
        showMigrationOption     = false;
        
        this.showVdbRolesImportOption = true;
        
        try  {
            jbInit();
            setWizardStuff(stepNum);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Constructor.  This constructor is used when a new vdb is being imported created 
     * (not versioning an existing vdb)
     * @param wizardInterface the wizard interface
     * @param stepNum the step number for this wizard panel
     */
    public VdbWizardEntitlementsSelectPanel(VirtualDatabase vdb, ConnectionInfo connection,
            WizardInterface wizardInterface, int stepNum)
    {
        super(wizardInterface);
        // Versioning a VDB, show the role migration option
        showMigrationOption     = true;

        this.vdbSourceVdb = vdb;
        
        VdbManager vdbManager = new VdbManager(connection);
        try {
			showVdbRolesImportOption = vdbManager.vdbHasDataRoles(this.vdbSourceVdb.getVirtualDatabaseID());
		} catch (Exception e1) {
			String msg = "Error getting VDB dataroles status"; //$NON-NLS-1$
			LogManager.logError(LogContexts.ROLES, e1, msg);
		}
        
        try  {
            jbInit();
            setWizardStuff(stepNum);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    /*
     * Constructor.  This constructor is used when a new vdb is being imported created 
     * (not versioning an existing vdb)
     * @param wizardInterface the wizard interface
     * @param stepNum the step number for this wizard panel
     */
    public VdbWizardEntitlementsSelectPanel(VirtualDatabase vdb, ConnectionInfo connection,
            WizardInterface wizardInterface, int stepNum, boolean showRolesImportOption)
    {
        super(wizardInterface);
        // Versioning a VDB, show the role migration option
        showMigrationOption     = true;

        this.vdbSourceVdb = vdb;
        
        showVdbRolesImportOption = showRolesImportOption;
        
        try  {
            jbInit();
            setWizardStuff(stepNum);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public VirtualDatabase getSourceVdb()
    {
        // only relevant to a 'version' create
        return vdbSourceVdb;
    }

    private void setWizardStuff(int stepNum) {
        setMainContent( pnlWorkspaceOuter );
        setStepText(stepNum, true, "Select the desired Role import or migration option.",null);
    }

    public String getVdbName()
    {
        return sVdbName;
    }

    public short getStatus()
    {
        return siStatus;
    }

    public void setVdbName( String sVdbName )
    {
        this.sVdbName = sVdbName;
    }

    public void setStatus( short siStatus )
    {
        this.siStatus = siStatus;
    }

    public void putDataIntoPanel()
    {
        txfName.setText( getVdbName() );
        short siStatus = getStatus();

        StringBuffer messageBuffer = new StringBuffer();
        // Set data based on VDB status
        if ( siStatus == VDBStatus.INCOMPLETE ) {
        	messageBuffer.append(INCOMPLETE_VDB_MESSAGE);
            txfStatus.setText("Incomplete");
        } else {
        	messageBuffer.append(INACTIVE_VDB_MESSAGE);
            txfStatus.setText("Inactive");
        }
        
        // Finish message based on whether migration is an option
        if(this.showMigrationOption) {
        	messageBuffer.append(ROLE_IMPORT_OR_MIGRATE_MESSAGE);
        } else {
        	messageBuffer.append(ROLE_IMPORT_MESSAGE);
        }
    	txaConfirmationMessage.setText( messageBuffer.toString() );
    }


    public boolean isSelectedNoEntitlementsImport()
    {
        return radioNoRolesImport.isSelected();
    }
    
    public boolean isSelectedImportEntitlementsFromVdb()
    {
        return radioImportVdbRoles.isSelected();
    }
    
    public boolean isSelectedMigrateEntitlements()
    {
        return radioMigrateVdbRoles.isSelected();
    }

    public boolean isSelectedViewEntitlementsReport()
    {

        return cbxViewVdbRolesReport.isSelected();
    }

    public void processMigrateEntitlementsCheckBoxAction() {
        // the 'view report' cbx is dependent on the 'migrate ent's' cbx:
        if (isSelectedMigrateEntitlements() || this.isSelectedImportEntitlementsFromVdb()) {
        	cbxViewVdbRolesReport.setEnabled(true);
        } else {
            if (cbxViewVdbRolesReport.isSelected()) {
                //unchecks button and notifies any listeners
            	cbxViewVdbRolesReport.doClick();
            }
            
            cbxViewVdbRolesReport.setEnabled(false);
        }
    }

        
    public CheckBox getViewEntitlementsReportCbx() {
        return cbxViewVdbRolesReport;
    }
    

    private void jbInit() throws Exception {
        border1 = new EmptyBorder(10,10,10,10);

        border1
            = new EmptyBorder(10,10,10,10);
        border3
            = new EmptyBorder(35,10,35,10);
        border5
            = new EmptyBorder(10,10,10,10);
        border6
            = new EmptyBorder(10,/*150*/10,10,10);
        pnlOuter.setLayout(borderLayout2);
        btnBack.setText("< Back");
        pnlBlather.setLayout(borderLayout1);
        borderLayout2.setVgap(5);
        pnlWorkSpace.setLayout(gridBagLayout1);
        btnCancel.setFont(new java.awt.Font("Dialog", 1, 12));
        btnCancel.setText("Finish");
        lblBlather.setFont(new java.awt.Font("Dialog", 1, 12));
        lblBlather.setBorder(border1);
        lblBlather.setText("Step 4: Click Finish to create this new Virtual Database");
        pnlButtons.setLayout(borderLayout4);
        gridLayout1.setColumns(3);
        gridLayout1.setHgap(50);
        pnlRealButtons.setLayout(gridLayout1);
        pnlWorkSpace.setBorder(border3);
        lblVersion.setHorizontalAlignment(SwingConstants.RIGHT);
        lblVersion.setText("Version: ");
        lblName.setHorizontalAlignment(SwingConstants.RIGHT);
        lblName.setText("Name:  ");

        //txvVersion.setBackground(Color.lightGray);
        //txvVersion.setEnabled(false);
        txvVersion.setText("1  ");
        txvVersion.setColumns(3);


        // PUT THIS IN WizardPanel: this.setTitle("Create New Virtual Database Wizard");
        //txfName.setBackground(Color.lightGray);
        if(getSourceVdb()!=null) {
        	txfName.setText(getSourceVdb().getName());
        }

        pnlWorkspaceOuter.setLayout(borderLayout3);
        //pnlConfirmationNotes.setBorder(border4);
        //pnlConfirmationNotes.setBorder(new BevelBorder(1));
        //lblConfirmationMessage.setBackground(Color.lightGray);
        
        // Confirmation Message TextArea
        txaConfirmationMessage.setBorder(border5);
        txaConfirmationMessage.setFont(new java.awt.Font("Dialog", 1, 12));
        txaConfirmationMessage.setText("");
        txaConfirmationMessage.setLineWrap(true);
        txaConfirmationMessage.setWrapStyleWord(true);
        txaConfirmationMessage.setEditable(false);
        txaConfirmationMessage.setBackground((new JPanel()).getBackground());
        
        txfStatus.setColumns(3);
        txfStatus.setText("Incomplete");
        //txfStatus.setEnabled(false);
        txfStatus.setFont(new java.awt.Font("SansSerif", 1, 12));
        //txfStatus.setBackground(Color.lightGray);
        lblStatus.setText("Status: ");
        lblStatus.setHorizontalAlignment(SwingConstants.RIGHT);
        pnlButtons.setBorder(border6);


        txfName.setEditable( false );
        txfStatus.setEditable( false );
        txvVersion.setEditable( false );
        //add(pnlOuter, BorderLayout.NORTH);
        //pnlOuter.add(pnlBlather, BorderLayout.NORTH);
        //pnlBlather.add(lblBlather, BorderLayout.NORTH);
        //pnlOuter.add(pnlButtons, BorderLayout.SOUTH);

        // DROP THE BUTTONS:
        //pnlButtons.add(pnlRealButtons, BorderLayout.CENTER);


        pnlRealButtons.add(btnBack, null);
        pnlRealButtons.add(btnCancel, null);
        //pnlOuter.add(pnlWorkspaceOuter, BorderLayout.CENTER);

        pnlWorkspaceOuter.add(pnlWorkSpace, BorderLayout.CENTER);

        // TEMPORARY FIX!!!!!!!!!!!!!!!!!!!!
        // set this to false until the wizard is redone so that it
        //  can go one more step correctly.....

        //cbxMigrateEntitlements.setEnabled( false );

        // end TEMPORARY FIX!!!!!!!!!!!!!!!!!!!!.

        // Import from VDB is default, so show report conrol is initially enabled
        cbxViewVdbRolesReport.setEnabled( true );

        radioNoRolesImport.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent event) {
		    	if(radioNoRolesImport.isSelected()) {
		    		processMigrateEntitlementsCheckBoxAction();
		    	}
            }
        });
        
        radioImportVdbRoles.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent event) {
		    	if(radioImportVdbRoles.isSelected()) {
		    		processMigrateEntitlementsCheckBoxAction();
		    	}
            }
        });

        radioMigrateVdbRoles.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent event) {
		    	if(radioMigrateVdbRoles.isSelected()) {
		    		processMigrateEntitlementsCheckBoxAction();
		    	}
            }
        });

        GridLayout gridLayout1   = new GridLayout();
        JPanel pnlRadios         = new JPanel( gridLayout1 );
        pnlRadios.setBorder(new EtchedBorder(1));
        gridLayout1.setVgap( 7 );
        gridLayout1.setRows( 4 );
        gridLayout1.setColumns( 1 );
        
        ButtonGroup group = new ButtonGroup();
        group.add(radioNoRolesImport);
        if(this.showVdbRolesImportOption) {
            group.add(radioImportVdbRoles);
        }
        if(this.showMigrationOption) {
        	group.add(radioMigrateVdbRoles);
        }
        
        // Determine initial radio selection
        // If vdb roles import is available, select it
        if(this.showVdbRolesImportOption) {
        	radioImportVdbRoles.setSelected(true);
        // Vdb roles not available, use Migrate if available
        } else if(this.showMigrationOption) {
            radioMigrateVdbRoles.setSelected(true);
        } else {
        	radioNoRolesImport.setSelected(true);
        }

        JPanel viewRolesRptPanel = new JPanel();
        JLabel dummyLabel = new JLabel(" "); //$NON-NLS-1$
        viewRolesRptPanel.add(dummyLabel);
        viewRolesRptPanel.add(cbxViewVdbRolesReport);

        pnlRadios.add( radioNoRolesImport );
        if(!this.showVdbRolesImportOption && !this.showMigrationOption) {
        	pnlRadios.add( new JLabel(NO_ROLES_AVAILABLE_TEXT));
        }
        if(this.showVdbRolesImportOption) {
        	pnlRadios.add( radioImportVdbRoles );
        }
        if(this.showMigrationOption) {
        	pnlRadios.add( radioMigrateVdbRoles );
        }
        if(this.showVdbRolesImportOption || this.showMigrationOption) {
        	pnlRadios.add( viewRolesRptPanel );
        }

        pnlWorkSpace.add(txaConfirmationMessage, new GridBagConstraints(0, 0, 2, 1, 1.0, 0.0
                ,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        
        pnlWorkSpace.add(lblName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0
            ,GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 49, 4));
        pnlWorkSpace.add(txfName, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0
            ,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 1), 177, 0));
        pnlWorkSpace.add(pnlRadios, new GridBagConstraints(1, 2, 2, 1, 0.0, 0.0
                ,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 0, 0));
        int iFinalYCoordinate = 3;

        pnlWorkSpace.add(new JPanel(), new GridBagConstraints(0, iFinalYCoordinate, GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER, 1.0, 1.0
            ,GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

    }
    
    public void setShowVdbRolesImportOptionEnabled(boolean showOption) {
    	this.showVdbRolesImportOption = showOption;
    	
    	// Change importVdbRoles text and set enabled state
		if(showOption) {
			radioImportVdbRoles.setText(" Import Role Definitions contained in the VDB");
			this.cbxViewVdbRolesReport.setEnabled(true);
		} else {
			radioImportVdbRoles.setText(" Import Role Definitions contained in the VDB [None available]");
			if(this.showMigrationOption || this.showVdbRolesImportOption) {
				this.cbxViewVdbRolesReport.setEnabled(true);
			} else {
				this.cbxViewVdbRolesReport.setEnabled(false);
			}
		}
		this.radioImportVdbRoles.setEnabled(showOption);
    	
		// Make default selection
		if(showOption) {
    		this.radioImportVdbRoles.setSelected(true);
    	} else if(this.showMigrationOption) {
    		this.radioMigrateVdbRoles.setSelected(true);
    	} else {
    		this.radioNoRolesImport.setSelected(true);
    	}
    }

}
