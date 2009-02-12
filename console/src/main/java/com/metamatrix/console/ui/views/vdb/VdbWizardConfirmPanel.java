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

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JPanel;
import javax.swing.JTextArea;

import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class VdbWizardConfirmPanel extends BasicWizardSubpanelContainer {

    // Messages
    private String NEED_ENTITLEMENTS_MESSAGE = "\n\nIf Roles are used at your installation, they "
                                              + "must be defined for a VDB before applications can use it.";

    private String INCOMPLETE_VDB_MESSAGE
	    =   "Please note: Since Connector Bindings were not specified for all " +
	        "of the models, the VDB will have an initial status of Incomplete, " +
	        "until you specify all Bindings.  ";

    private String INACTIVE_VDB_MESSAGE
	    =   "Please note: Connector Bindings have been specified for all " +
	        "of the models.  The VDB will have an initial status of Inactive.  ";

    private final static int NOTES_HORIZONTAL_MARGINS = 20;
    private final static int LABELS_HORIZONTAL_MARGINS = 5;

    private TextFieldWidget txfName;
    private JPanel thePanel;
    private JTextArea txaConfirmationMessage;
    private TextFieldWidget txfStatus;
    private CheckBox cbxSetStatusActive;
    private CheckBox cbxSync;

    // data
    private String sVdbName = "";
    private short siStatus = 0;

    // used when this panel is doing a new version
    private VirtualDatabase vdbSourceVdb = null;

    private boolean NEW_VDB = true;
    private boolean NEW_VDB_VERSION = false;

    private boolean bMigrateEntitlementsRequested = false;

    public VdbWizardConfirmPanel(WizardInterface wizardInterface,
                                 int stepNum) {
        super(wizardInterface);
        NEW_VDB_VERSION = false;
        NEW_VDB = true;
        try {
            jbInit();
            setWizardStuff(stepNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public VdbWizardConfirmPanel(VirtualDatabase vdb,
                                 WizardInterface wizardInterface,
                                 int stepNum) {
        super(wizardInterface);
        NEW_VDB_VERSION = true;
        NEW_VDB = false;

        this.vdbSourceVdb = vdb;
        try {
            jbInit();
            setWizardStuff(stepNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public VirtualDatabase getSourceVdb() {
        // only relevant to a 'version' create
        return vdbSourceVdb;
    }

    private void setWizardStuff(int stepNum) {
        setMainContent(thePanel);
        int numSteps = getWizardInterface().getPageCount();
        boolean isLastStep = (stepNum == numSteps - 1);
        if (isLastStep) {
            if (NEW_VDB) {
                setStepText(stepNum, "Click \"Finish\" to create the new Virtual Database.");
            } else {
                setStepText(stepNum, "Click \"Finish\" to create a new version of the Virtual Database "
                                     + getSourceVdb().getName()
                                     + ".");
            }
        } else {
            if (NEW_VDB) {
                setStepText(stepNum, "Click \"Next\" to create the new Virtual Database.");
            } else {
                setStepText(stepNum, "Click \"Next\" to create a new version of the Virtual Database "
                                     + getSourceVdb().getName()
                                     + ".");
            }
        }
    }

    public String getVdbName() {
        return sVdbName;
    }

    public short getStatus() {
        if (isSelectedSetStatusActive()) {
            return VDBStatus.ACTIVE;
        }

        return siStatus;
    }

    public void setVdbName(String sVdbName) {
        this.sVdbName = sVdbName;
    }

    public void setStatus(short siStatus) {
        this.siStatus = siStatus;
    }

    // the call of this method is to initialize the data based on
    // the status not in the active status,
    public void putDataIntoPanel() {
        txfName.setText(getVdbName());
        String text = "";
        // wasMigrateEntitlementsRequested

        if (siStatus == VDBStatus.INCOMPLETE) {
            if (NEW_VDB) {
                text = INCOMPLETE_VDB_MESSAGE + NEED_ENTITLEMENTS_MESSAGE;
            } else if (NEW_VDB_VERSION) {
                text = INCOMPLETE_VDB_MESSAGE;
                if (!wasMigrateEntitlementsRequested()) {
                    text += NEED_ENTITLEMENTS_MESSAGE;
                }
            }
            txfStatus.setText("Incomplete");
            cbxSetStatusActive.setVisible(false);
            cbxSync.setVisible(false);
        } else {
            if (NEW_VDB) {
                text = INACTIVE_VDB_MESSAGE + NEED_ENTITLEMENTS_MESSAGE;
            } else if (NEW_VDB_VERSION) {
                text = INACTIVE_VDB_MESSAGE;
                if (!wasMigrateEntitlementsRequested()) {
                    text += NEED_ENTITLEMENTS_MESSAGE;
                }
            }
            txfStatus.setText("Inactive");
            cbxSetStatusActive.setVisible(true);
            cbxSync.setVisible(true);
        }
        txaConfirmationMessage.setText(text);
    }

    public void setMigrateEntitlementsRequested(boolean b) {
        bMigrateEntitlementsRequested = b;
    }

    public boolean wasMigrateEntitlementsRequested() {
        return bMigrateEntitlementsRequested;
    }

    private void jbInit() throws Exception {
        thePanel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        thePanel.setLayout(layout);
        
        // Confirmation Message TextArea
        txaConfirmationMessage = new JTextArea();
        //txaConfirmationMessage.setBorder(new EmptyBorder(10,10,10,10));
        txaConfirmationMessage.setFont(new java.awt.Font("Dialog", 1, 12));
        txaConfirmationMessage.setText("");
        txaConfirmationMessage.setLineWrap(true);
        txaConfirmationMessage.setWrapStyleWord(true);
        txaConfirmationMessage.setEditable(false);
        txaConfirmationMessage.setBackground((new JPanel()).getBackground());

        thePanel.add(txaConfirmationMessage);
        layout.setConstraints(txaConfirmationMessage, new GridBagConstraints(0, 0, 4, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                                                                             GridBagConstraints.HORIZONTAL,
                                                                             new Insets(10, NOTES_HORIZONTAL_MARGINS, 10,
                                                                                        NOTES_HORIZONTAL_MARGINS), 0, 0));

        LabelWidget lblName = new LabelWidget("Name:");
        thePanel.add(lblName);
        layout.setConstraints(lblName, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                                                              GridBagConstraints.NONE, new Insets(0, LABELS_HORIZONTAL_MARGINS,
                                                                                                  4, 0), 0, 0));
        txfName = new TextFieldWidget();
        txfName.setEditable(false);
        thePanel.add(txfName);
        layout.setConstraints(txfName, new GridBagConstraints(1, 1, 3, 1, 0.0, 0.0, GridBagConstraints.NORTHWEST,
                                                              GridBagConstraints.HORIZONTAL,
                                                              new Insets(0, 2, 4, LABELS_HORIZONTAL_MARGINS), 0, 0));

        LabelWidget lblStatus = new LabelWidget("Status:");
        thePanel.add(lblStatus);
        layout.setConstraints(lblStatus, new GridBagConstraints(0, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTHWEST,
                                                                GridBagConstraints.NONE, new Insets(4, LABELS_HORIZONTAL_MARGINS,
                                                                                                    4, 0), 0, 0));
        txfStatus = new TextFieldWidget();
        txfStatus.setEditable(false);
        txfStatus.setFont(new Font("SansSerif", 1, 12));
        thePanel.add(txfStatus);
        layout.setConstraints(txfStatus, new GridBagConstraints(1, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTH,
                                                                GridBagConstraints.NONE, new Insets(4, 2, 4, 0), 0, 0));

        cbxSetStatusActive = new CheckBox("Set Status Active", CheckBox.SELECTED);
        thePanel.add(cbxSetStatusActive);
        layout.setConstraints(cbxSetStatusActive, new GridBagConstraints(2, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTHWEST,
                                                                         GridBagConstraints.NONE,
                                                                         new Insets(4, 15, 4, LABELS_HORIZONTAL_MARGINS), 0, 0));

        cbxSync = new CheckBox("Synchronize", CheckBox.SELECTED);
        thePanel.add(cbxSync);
        layout.setConstraints(cbxSync, new GridBagConstraints(3, 2, 1, 1, 1.0, 1.0, GridBagConstraints.NORTHWEST,
                                                              GridBagConstraints.NONE, new Insets(4, 15, 4,
                                                                                                  LABELS_HORIZONTAL_MARGINS), 0,
                                                              0));
    }

    public boolean isSelectedSetStatusActive() {
        return (cbxSetStatusActive.isSelected() && cbxSetStatusActive.isVisible());
    }

    public boolean isSyncActive() {
        return (cbxSync.isSelected() && cbxSync.isVisible());
    }
}// end VdbWizardConfirmPanel
