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

//#############################################################################
package com.metamatrix.console.ui.views.deploy;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;

public class ConfirmationPanel
    extends DialogPanel {

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private int nextRow = 0;
    private GridBagConstraints gbc = new GridBagConstraints();

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private JPanel pnl;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public ConfirmationPanel(String theMsgId) {
        this(theMsgId, "icon.warning", "cp.btnOk", "cp.btnCancel"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public ConfirmationPanel(
        String theMsgId,
        Object[] theMsgParams) {

        this(theMsgId, theMsgParams, "icon.warning", "cp.btnOk", "cp.btnCancel"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public ConfirmationPanel(
        String theMsgId,
        String theIconId,
        String theOkId,
        String theCancelId) {

        this(theMsgId, null, theIconId, theOkId, theCancelId);
    }

    public ConfirmationPanel(
        String theMsgId,
        Object[] theMsgParams,
        String theIconId,
        String theOkId,
        String theCancelId) {

        super();
        pnl = new JPanel(new GridBagLayout());
        pnl.setBorder(BorderFactory.createEmptyBorder(30, 40, 30, 40));
        String txt = (theMsgParams == null)
                         ? DeployPkgUtils.getString(theMsgId)
                         : DeployPkgUtils.getString(theMsgId, theMsgParams);
        JLabel lbl = new LabelWidget(txt);
        if (theIconId != null) {
            lbl.setIcon(DeployPkgUtils.getIcon(theIconId));
        }
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.gridx = 0;
        gbc.gridy = nextRow;
        pnl.add(lbl, gbc);
        setContent(pnl);
        ButtonWidget btnOk = getAcceptButton();
        btnOk.setText(DeployPkgUtils.getString(theOkId));
        ButtonWidget btnCancel = getCancelButton();
        btnCancel.setText(DeployPkgUtils.getString(theCancelId));
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected void addContent(JComponent theComponent) {
        gbc.gridx = 0;
        gbc.gridy++;
        pnl.add(theComponent, gbc);
    }

    public boolean isConfirmed() {
        return getSelectedButton() == getAcceptButton();
    }

}
