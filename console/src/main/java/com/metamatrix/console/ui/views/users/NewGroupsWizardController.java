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
import java.awt.Dimension;
import java.awt.Point;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.JDialog;

import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

public class NewGroupsWizardController extends WizardInterfaceImpl {

    private final static int CONNECTOR_SELECT_PAGE_NUM = 0;

    private NewGroupsWizardSelectionPanel groupsSelectionPanel;

    private JDialog dialog;
    private int currentPage = -1;
    // private ButtonWidget backButton;
    private ButtonWidget cancelButton;
    private ButtonWidget finishButton;
    private List selectedGroups = new ArrayList();
    private GroupsManager groupsManager;

    public NewGroupsWizardController(GroupsManager manager) {
        super();
        
        this.groupsManager = manager;
    }

    private GroupsManager getGroupsManager() {
        return this.groupsManager;
    }

    public List runWizard(Collection listToRemoveFromAvailable) {
    	groupsSelectionPanel = new NewGroupsWizardSelectionPanel(this, getGroupsManager(), listToRemoveFromAvailable);

        if ((groupsSelectionPanel != null)) {

            addPage(groupsSelectionPanel);

            dialog = new JDialog(ConsoleMainFrame.getInstance(), "Select Groups"); //$NON-NLS-1$
            dialog.setModal(true);
            
            Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
            int preferredWidth = (int)(screenSize.width * 0.25);
            int preferredHeight = (int)(screenSize.width * 0.25);
            
            setSize(new Dimension(preferredWidth,preferredHeight));

            cancelButton = getCancelButton();
            cancelButton.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent ev) {
                    cancelPressed();
                }
            });

            // backButton =
            getBackButton();

            finishButton = getFinishButton();
            finishButton.addActionListener(new ActionListener() {

                public void actionPerformed(ActionEvent ev) {
                    finishPressed();
                }
            });

            getNextButton();

            dialog.getContentPane().add(this);
            dialog.addWindowListener(new WindowAdapter() {

                public void windowClosing(WindowEvent ev) {
                    cancelPressed();
                }
            });
            dialog.pack();

            currentPage = CONNECTOR_SELECT_PAGE_NUM;

            setLocationOn(dialog);
            dialog.show();
        }
        return selectedGroups;
    }

    private void cancelPressed() {
    	selectedGroups.clear();
        dialog.dispose();
    }

    private void finishPressed() {
    	selectedGroups = groupsSelectionPanel.getSelectedGroups();
        dialog.dispose();
    }

    public JDialog getDialog() {
        return dialog;
    }

    public void showNextPage() {
        boolean bContinue = false;
        try {
            StaticUtilities.startWait(getDialog().getContentPane());
            bContinue = goingToNextPage();
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed testing new connector binding name for uniqueness", e); //$NON-NLS-1$
        } finally {
            StaticUtilities.endWait(getDialog().getContentPane());
        }
        if (bContinue) {
            currentPage += 1;
            super.showNextPage();
        }
    }

    public void showPreviousPage() {
        goingToPreviousPage();
        currentPage -= 1;
        super.showPreviousPage();
    }

    public Dimension getPreferredSize() {
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension unadjusted = super.getPreferredSize();
        return new Dimension(Math.max(unadjusted.width, (int)(screenSize.width * 0.6)), 
        		              Math.max(unadjusted.height, (int)(screenSize.height * 0.6)));
    }

    public boolean goingToNextPage() {
        boolean bContinue = true;
        switch (currentPage) {
            case CONNECTOR_SELECT_PAGE_NUM:
                break;

        }
        return bContinue;
    }

    public void goingToPreviousPage() {
        enableNextButton(true);
    }

    public static void setLocationOn(Component comp) {
        Point p = StaticUtilities.centerFrame(comp.getSize());
        comp.setLocation(p.x, p.y);
    }

    public void enableNextButton(boolean b) {
        ButtonWidget btnNext = null;
        btnNext = getNextButton();
        if (btnNext != null) {
            btnNext.setEnabled(b);
        } else {
            btnNext = getFinishButton();
            if (btnNext != null) {
                btnNext.setEnabled(b);
            }
        }
    }

}
