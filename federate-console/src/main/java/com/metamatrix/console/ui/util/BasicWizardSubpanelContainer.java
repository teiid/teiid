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

package com.metamatrix.console.ui.util;

import java.awt.*;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JPanel;

import com.metamatrix.console.ui.layout.BasePanel;

public class BasicWizardSubpanelContainer extends BasePanel {
    private JPanel lblStep;
    private JPanel pnlMainContent;
    private WizardInterface wizardInterface;
    private WizardStepTextPanel stepTextPanel;

    public BasicWizardSubpanelContainer(WizardInterface wizInterface) {
        super();
        wizardInterface = wizInterface;
        setLayout(new GridBagLayout());

        JPanel pnlStep = new JPanel(new GridLayout(1, 1));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        add(pnlStep, gbc);

        lblStep = new JPanel();
        pnlStep.add(lblStep);

        pnlMainContent = new JPanel(new GridLayout(1, 1));
        pnlMainContent.setBorder(
            BorderFactory.createCompoundBorder(
                BorderFactory.createEtchedBorder(),
                BorderFactory.createEmptyBorder(3, 5, 3, 5)));
        gbc.gridy = 1;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        add(pnlMainContent, gbc);
    }

    public WizardInterface getWizardInterface() {
        return wizardInterface;
    }
    
    public void enableForwardButton(boolean bEnableState) {
        AbstractButton forwardButton = wizardInterface.getForwardButton();
        forwardButton.setEnabled(bEnableState);
    }

    public boolean isForwardButtonEnabled() {
        AbstractButton forwardButton = wizardInterface.getForwardButton();
        return forwardButton.isEnabled();
    }
    
    public void resolveForwardButton() {
        // Default behavior; subclasses will override this
        //  if they wish to make the next button dependent on
        //  some internal data state of their own:
        enableForwardButton(true);
    }

    public void setStepText(int stepNum, boolean optional, String header,
    		String[] paragraphs) {
        lblStep.removeAll();
        lblStep.setLayout(new GridLayout(1, 1));
        stepTextPanel = new WizardStepTextPanel(stepNum, optional, header, paragraphs);
        lblStep.add(stepTextPanel);
    }

	public void setStepText(int stepNum, String header) {
		setStepText(stepNum, false, header, null);
	}
    
    public int getStepNum() {
        int stepNum = -1;
        if (stepTextPanel != null) {
            stepNum = stepTextPanel.getStepNum();
        }
        return stepNum;
    }
    
    public void replaceStepNum(int newStepNum) {
        if (stepTextPanel != null) {
            stepTextPanel.replaceStepNum(newStepNum);
        }
    }
	
    public String getEntireStepText() {
        return stepTextPanel.getText();
    }
    
    public void setMainContent(JComponent cmpComponent) {
        pnlMainContent.add(cmpComponent);
    }
}
