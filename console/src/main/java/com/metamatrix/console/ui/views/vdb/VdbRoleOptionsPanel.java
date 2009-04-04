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

package com.metamatrix.console.ui.views.vdb;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;

import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;


/*
 * Panel for selection of Role Import options
 */
public class VdbRoleOptionsPanel extends BasicWizardSubpanelContainer {
	
	private static final String SPACE = " "; //$NON-NLS-1$
	
    private ButtonWidget btnBack = new ButtonWidget();
    private JPanel pnlMessages = new JPanel();
    private JPanel pnlWorkSpace = new JPanel();
    private ButtonWidget btnCancel = new ButtonWidget();
    private JPanel pnlButtons = new JPanel();
    private GridLayout gridLayout1 = new GridLayout();
    private JPanel pnlRealButtons = new JPanel();
    private Border border3;
    private GridBagLayout gridBagLayout1 = new GridBagLayout();
    private JPanel pnlWorkspaceOuter = new JPanel();
    private BorderLayout borderLayout3 = new BorderLayout();
    private JTextArea txaImportOptionMessage = new JTextArea();
    private Border border5;
    private Border border6;

    // data
    private String sVdbName         = "";

    // Role import option widgets
    private AbstractButton radioDontOverwriteExistingRoles;
    private AbstractButton radioOverwriteExistingRoles;
    
    // VDB Status messages
    private String RESOLVE_CONFLICTS_MESSAGE = ConsolePlugin.Util.getString("VdbRoleOptionsPanel.resolveConflictsMessage.text"); //$NON-NLS-1$
    
    // used when this panel is doing a new version
    VirtualDatabase vdbSourceVdb        = null;

    /*
     * Constructor.  
     * @param vdb the vdb onto which the roles are being applied
     * @param wizardInterface the wizard interface
     * @param stepNum the step number for this wizard panel
     */
    public VdbRoleOptionsPanel(VirtualDatabase vdb,
            WizardInterface wizardInterface, int stepNum)
    {
        super(wizardInterface);

        this.vdbSourceVdb = vdb;
        try  {
            jbInit();
            setWizardStuff(stepNum);
        } catch(Exception ex) {
        	LogManager.logError(LogContexts.INITIALIZATION, ex, ex.getMessage());
        }
    }

    public VirtualDatabase getSourceVdb()
    {
        // only relevant to a 'version' create
        return vdbSourceVdb;
    }

    private void setWizardStuff(int stepNum) {
        setMainContent( pnlWorkspaceOuter );
        setStepText(stepNum, false, ConsolePlugin.Util.getString("VdbRoleOptionsPanel.stepText"),null);
    }

    public String getVdbName()
    {
        return sVdbName;
    }

    public void setVdbName( String sVdbName )
    {
        this.sVdbName = sVdbName;
    }

    public boolean isSelectedOverwriteExistingRoles()
    {
        return radioOverwriteExistingRoles.isSelected();
    }

    private void jbInit() throws Exception {
        border3
            = new EmptyBorder(35,10,35,10);
        border5
            = new EmptyBorder(10,10,10,10);
        border6
            = new EmptyBorder(10,/*150*/10,10,10);
        
        BorderLayout borderLayout1 = new BorderLayout();
        BorderLayout borderLayout2 = new BorderLayout();
        BorderLayout borderLayout4 = new BorderLayout();

        JPanel pnlOuter = new JPanel();
        pnlOuter.setLayout(borderLayout2);
        btnBack.setText("< Back");
        pnlMessages.setLayout(borderLayout1);
        borderLayout2.setVgap(5);
        pnlWorkSpace.setLayout(gridBagLayout1);
        btnCancel.setFont(new java.awt.Font("Dialog", 1, 12));
        btnCancel.setText("Finish");

        pnlButtons.setLayout(borderLayout4);
        gridLayout1.setColumns(3);
        gridLayout1.setHgap(50);
        pnlRealButtons.setLayout(gridLayout1);
        pnlWorkSpace.setBorder(border3);

        pnlWorkspaceOuter.setLayout(borderLayout3);
        
        // Confirmation Message TextArea
        txaImportOptionMessage.setBorder(border5);
        txaImportOptionMessage.setFont(new java.awt.Font("Dialog", 1, 12));
        txaImportOptionMessage.setText("");
        txaImportOptionMessage.setLineWrap(true);
        txaImportOptionMessage.setWrapStyleWord(true);
        txaImportOptionMessage.setEditable(false);
        txaImportOptionMessage.setBackground((new JPanel()).getBackground());
        txaImportOptionMessage.setText(RESOLVE_CONFLICTS_MESSAGE);
        
        pnlButtons.setBorder(border6);

        pnlRealButtons.add(btnBack, null);
        pnlRealButtons.add(btnCancel, null);

        pnlWorkspaceOuter.add(pnlWorkSpace, BorderLayout.CENTER);

        GridLayout gridLayout1      = new GridLayout();
        JPanel pnlCheckBoxes        = new JPanel( gridLayout1 );
        gridLayout1.setVgap( 7 );
        gridLayout1.setRows( 4 );
        gridLayout1.setColumns( 1 );
        
        ButtonGroup group = new ButtonGroup();
        radioDontOverwriteExistingRoles = new JRadioButton(SPACE+ConsolePlugin.Util.getString("VdbRoleOptionsPanel.radioDontOverwriteRoles.text"));
        radioOverwriteExistingRoles = new JRadioButton(SPACE+ConsolePlugin.Util.getString("VdbRoleOptionsPanel.radioOverwriteRoles.text"));
        group.add(radioDontOverwriteExistingRoles);
        group.add(radioOverwriteExistingRoles);
        radioDontOverwriteExistingRoles.setSelected(true);

        pnlCheckBoxes.add( radioDontOverwriteExistingRoles );
        pnlCheckBoxes.add( radioOverwriteExistingRoles );

        pnlWorkSpace.add(txaImportOptionMessage, new GridBagConstraints(0, 0, 2, 1, 1.0, 0.0
                ,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        
        pnlWorkSpace.add(pnlCheckBoxes, new GridBagConstraints(1, 1, 2, 2, 0.0, 0.0
                ,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 0, 0));
        int iFinalYCoordinate = 2;

        pnlWorkSpace.add(new JPanel(), new GridBagConstraints(0, iFinalYCoordinate, GridBagConstraints.REMAINDER, GridBagConstraints.REMAINDER, 1.0, 1.0
            ,GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

    }



}
