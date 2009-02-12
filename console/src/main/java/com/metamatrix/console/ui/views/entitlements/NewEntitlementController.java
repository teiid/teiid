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

package com.metamatrix.console.ui.views.entitlements;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JDialog;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;

public class NewEntitlementController {
    public final static int NOT_SHOWN = 1;
    public final static int DISABLED = 2;
    public final static int ENABLED = 3;

    public final static int CANCELLED = 1;
    public final static int NEXT_PRESSED = 2;
    public final static int FINISHED = 3;
    public final static int BACK_PRESSED = 4;
    public final static int HELP_PRESSED = 5;

    private EntitlementsPanel caller;
    private NewEntitlementWizardPanel wizard;
    private JDialog dialog;
    private boolean created = false;
    private NewEntitlementNamePanel firstPanel;
    private NewEntitlementBasedOnPanel secondPanel,thirdPanel;
    private NewEntitlementConfirmationPanel fourthPanel;
    private EntitlementsDataInterface dataSource;
    private int titleStepNum = 2;
    private String title = "Select existing roles on which to base " +
                "authorizations.";
    private String[] titleParagraphs = new String[] {
    		"Authorizations for the new role will be initially set to match "+
            "authorizations for the selected role, for all data nodes existing in both roles."};
    private int pTitleStepNum = 3;
    private String pTitle = "Select existing roles on which to base " +
            "groups.";
    private String[] pTitleParagraphs = new String[] {
    		"groups for the new role will be initially set to match " +
            "groups for the selected role."} ;
                
    public NewEntitlementController(EntitlementsPanel callr,
            EntitlementsDataInterface dataSrc)
            throws AuthorizationException, ExternalException,
            ComponentNotFoundException {
        super();
        caller = callr;
        dataSource = dataSrc;
        init();
    }

    private void init()
            throws AuthorizationException, ExternalException,
            ComponentNotFoundException {
        wizard = new NewEntitlementWizardPanel(this);
//        AbstractButton wizardNextButton = wizard.getNextButton();
        firstPanel = new NewEntitlementNamePanel(dataSource, this, wizard);
        secondPanel = new NewEntitlementBasedOnPanel(titleStepNum, title,
        		titleParagraphs,
                (EntitlementsTableModel)caller.getTable().getModel(), wizard);
        thirdPanel = new NewEntitlementBasedOnPanel(pTitleStepNum, pTitle,
        		pTitleParagraphs, 
                (EntitlementsTableModel)caller.getTable().getModel(), wizard);
        fourthPanel = new NewEntitlementConfirmationPanel(wizard);
        wizard.addPage(firstPanel);
        wizard.addPage(secondPanel);
        wizard.addPage(thirdPanel);
        wizard.addPage(fourthPanel);
        wizard.getFinishButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                finish();
                dialog.dispose();
            }
        });
        wizard.getCancelButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                dialog.dispose();
            }
        });
    }

    public void go() {
        dialog = new JDialog(ConsoleMainFrame.getInstance(),
                "Create New Role Wizard");
        dialog.setModal(true);
        dialog.getContentPane().add(wizard);
        dialog.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                dialog.dispose();
            }
        });
        dialog.pack();
        Dimension size = dialog.getSize();
        dialog.setSize(size.width, Math.min(size.height,
                (int)(Toolkit.getDefaultToolkit().getScreenSize().height * 0.6)));
        dialog.setLocation(StaticUtilities.centerFrame(dialog.getSize()));
        dialog.show();
    }

    public boolean showNextPage() {
        boolean proceeding = true;
        Component comp = wizard.getCurrentPage();
        if (comp == secondPanel) {
            String entName = getEntitlementName();
            String entDescription = getEntitlementDescription();
            String vdbName = getVDBName();
            int vdbVersion = getVDBVersion();
            EntitlementsTableRowData dataNodesEnt = getDataNodesEntitlement();
//            EntitlementsTableRowData principalsEnt =
                    getPrincipalsEntitlement();
            fourthPanel.setTexts(entName, entDescription, vdbName,
                    (new Integer(vdbVersion)).toString(),
                    dataNodesEnt.getEntitlementName(),
                    dataNodesEnt.getVDBName(),
                    dataNodesEnt.getVDBVersion());
         } else if (comp == thirdPanel) {
            EntitlementsTableRowData principalsEnt =
                    getPrincipalsEntitlement();
            fourthPanel.setPrincipalsTexts(
                    principalsEnt.getEntitlementName(),
                    principalsEnt.getVDBName(),
                    principalsEnt.getVDBVersion());
        } else if (comp == firstPanel) {
            String entName = getEntitlementName();
            String vdbName = getVDBName();
            int vdbVersion = getVDBVersion();
            boolean exists = false;
            try {
                exists = caller.doesEntitlementExist(entName, vdbName, vdbVersion);
            } catch (Exception ex) {
                LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                        "Error determining if role exists.");
                ExceptionUtility.showMessage("Determine if role exists", ex);
            }
            if (exists) {
                proceeding = false;
                StaticUtilities.displayModalDialogWithOK("Role Already Exists",
                        "Role \"" + entName + "\" for VDB \"" +
                        vdbName + "\" version " + vdbVersion + " already exists.  " +
                        "Must enter a different role name or select " +
                        "another VDB.");
            }
        }
        return proceeding;
    }

    public void showPreviousPage() {
        //Until proven otherwise, there is nothing to do here
    }

    public void finish() {
        created = true;
    }

    public boolean isCreated() {
        return created;
    }

    public String getEntitlementName() {
        String entName = firstPanel.getEntitlementName().trim();
        return entName;
    }

    public String getEntitlementDescription() {
        String entDescription = firstPanel.getEntitlementDescription().trim();
        return entDescription;
    }

    public String getVDBName() {
        String vdbName = firstPanel.getVDBName().trim();
        return vdbName;
    }

    public int getVDBVersion() {
        int vdbVersion = firstPanel.getVDBVersion();
        return vdbVersion;
    }

    public EntitlementsTableRowData getDataNodesEntitlement() {
        EntitlementsTableRowData dataNodesEnt =
                secondPanel.getDataNodesSelectionRowData();
        return dataNodesEnt;
    }

    public EntitlementsTableRowData getPrincipalsEntitlement() {
       EntitlementsTableRowData dataNodesEnt =
                thirdPanel.getDataNodesSelectionRowData();
        return dataNodesEnt;
    }
}
