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

package com.metamatrix.console.ui.views.authorization;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JDialog;

import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.AuthenticationProviderManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/**
 * New AuthenticationProvider WizardController
 */
public class NewAuthenticationProviderWizardController extends WizardInterfaceImpl {

    private final static int PROVIDER_SELECT_PAGE_NUM = 0;

    private NewAuthenticationProviderSelectPanel providerSelectPanel;
    private ComponentType currentlySelectedProvider = null;
    private String sNewCBName = null;
    private NewAuthenticationProviderSpecificationPanel specsPanel;

    private JDialog dialog;
    private int currentPage = -1;
    // private ButtonWidget backButton;
    private ButtonWidget nextButton;
    private ButtonWidget cancelButton;
    private ButtonWidget finishButton;
    private AuthenticationProvider scdNewAuthenticationProvider = null;

    private ConnectionInfo connection;

    /**
     * Constructor
     * @param connection the ConnectionInfo
     */
    public NewAuthenticationProviderWizardController(ConnectionInfo connection) {
        super();
        this.connection = connection;
    }

    private AuthenticationProviderManager getAuthenticationProviderManager() {
        return ModelManager.getAuthenticationProviderManager(connection);
    }

    public AuthenticationProvider runWizard() {
    	providerSelectPanel = new NewAuthenticationProviderSelectPanel(this, connection);
        specsPanel = new NewAuthenticationProviderSpecificationPanel(this, connection);

        if ((providerSelectPanel != null) && (specsPanel != null)) {

            addPage(providerSelectPanel);
            addPage(specsPanel);

            dialog = new JDialog(ConsoleMainFrame.getInstance(), "Create New Membership Domain Provider Wizard"); //$NON-NLS-1$
            dialog.setModal(true);
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

            nextButton = getNextButton();

            dialog.getContentPane().add(this);
            dialog.addWindowListener(new WindowAdapter() {

                public void windowClosing(WindowEvent ev) {
                    cancelPressed();
                }
            });
            dialog.pack();

            currentPage = PROVIDER_SELECT_PAGE_NUM;

            setLocationOn(dialog);
            dialog.show();
        }
        return scdNewAuthenticationProvider;
    }

    private void cancelPressed() {
        dialog.dispose();
    }

    public void importFileSelected() {
        nextButton.setEnabled(true);
    }

    private void finishPressed() {
    	createAuthenticationProvider();
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
            ExceptionUtility.showMessage("Failed testing new provider name for uniqueness", e); //$NON-NLS-1$
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
        return new Dimension(Math.max(unadjusted.width, (int)(screenSize.width * 0.7)), unadjusted.height);
    }

    public boolean goingToNextPage() {
        boolean bContinue = true;
        switch (currentPage) {
            case PROVIDER_SELECT_PAGE_NUM:
                boolean bNameIsUnique = false;
                bNameIsUnique = nameIsUnique();

                // verify that the new name is not already being used:
                if (bNameIsUnique) {
                    ComponentType connector = providerSelectPanel.getSelectedAuthenticationProvider();

                    boolean typeChanged = (!connector.equals(currentlySelectedProvider));
                    boolean nameChanged = ((sNewCBName == null) || (!sNewCBName.equals(providerSelectPanel.getNewProviderName())));

                    if (typeChanged || nameChanged) {

                        // if connector type has changed, completely re-init the specsPanel
                        try {
                            StaticUtilities.startWait(dialog.getContentPane());
                            specsPanel.updatePropertiedObjectPanel(providerSelectPanel.getSelectedAuthenticationProvider(),
                            		providerSelectPanel.getNewProviderName(),typeChanged);
                        } finally {
                            StaticUtilities.endWait(dialog.getContentPane());
                        }
                        currentlySelectedProvider = connector;
                        sNewCBName = providerSelectPanel.getNewProviderName();
                    }
                }
                bContinue = bNameIsUnique;

                // try turning ON the next button, then see if 'setButtons'
                // can turn it off when appropriate:
                // VERY special case. If we are going to go forward, but
                // we need to call 'setButtons' AFTER we go to the next page,
                // then we can set call bContinue to false

                // if bContinue is true, then go on to next page here,
                // and then try to set the Next button on the new page;
                // when done, turn the bContinue flag off so that 'showNextPage'
                // is not done one time too many...
                if (bContinue) {
                    enableNextButton(true);
                    specsPanel.setButtons();
                }
                break;

        }
        return bContinue;
    }

    public void goingToPreviousPage() {
        enableNextButton(true);
    }

    private boolean nameIsUnique() {
        String sName = providerSelectPanel.getNewProviderName();
        boolean bNameNotUnique = false;

        try {
            StaticUtilities.startWait(dialog.getContentPane());
            bNameNotUnique = getAuthenticationProviderManager().providerTypeNameAlreadyExists(sName);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed testing new provider name for uniqueness", e); //$NON-NLS-1$
        } finally {
            StaticUtilities.endWait(dialog.getContentPane());
        }

        if (bNameNotUnique) {
            StaticUtilities.displayModalDialogWithOK("Domain Provider Already Exists", //$NON-NLS-1$
                                                     "A Provider already exists with the name \"" //$NON-NLS-1$
                                                      + sName + "\".  Please enter a unique name."); //$NON-NLS-1$
        } 
        return !(bNameNotUnique);
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

    private void createAuthenticationProvider() {
        AuthenticationProvider provider = specsPanel.getNewAuthenticationProvider();
        ConfigurationObjectEditor coe = specsPanel.getConfigurationObjectEditor();
        try {
            getAuthenticationProviderManager().createAuthenticationProvider(provider, coe);
            scdNewAuthenticationProvider = provider;
            //ModelManager.getMembershipAPI(this.connection).addDomain(provider.getName(),provider.getProperties());
        } catch (Exception ex) {
            String msg = "Error creating Membership Domain provider."; //$NON-NLS-1$
            LogManager.logError(LogContexts.CONNECTOR_BINDINGS, ex, msg);
            ExceptionUtility.showMessage(msg, ex);
        }
    }
}
