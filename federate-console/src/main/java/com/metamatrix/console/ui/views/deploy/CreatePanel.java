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

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.KeyStroke;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public final class CreatePanel
    extends ConfirmationPanel
    implements ActionListener,
               DocumentListener {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static final KeyStroke ENTER_RELEASED =
        KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0, true);

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TextFieldWidget txf;
    private ButtonWidget btnCreate;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public CreatePanel(
        String theMessageId,
        String theIconId,
        String theLabelId,
        String theNameTypeId) {

        super(theMessageId, theIconId, "rp.btnCreate", "rp.btnCancel"); //$NON-NLS-1$ //$NON-NLS-2$
        JPanel pnl = new JPanel();
        JLabel lbl = new LabelWidget(DeployPkgUtils.getString(theLabelId));
        pnl.add(lbl);
        txf = DeployPkgUtils.createTextField(theNameTypeId);
        txf.getDocument().addDocumentListener(this);
        txf.registerKeyboardAction(this, ENTER_RELEASED, WHEN_FOCUSED);
        pnl.add(txf);
        addContent(pnl);
        btnCreate = getAcceptButton();
        btnCreate.setEnabled(false);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public void actionPerformed(ActionEvent theEvent) {
        btnCreate.doClick();
    }

    public void changedUpdate(DocumentEvent theEvent) {
        if ((txf.getText().length() > 0) && !btnCreate.isEnabled()) {
            btnCreate.setEnabled(true);
        }
        else if ((txf.getText().length() == 0) && btnCreate.isEnabled()) {
            btnCreate.setEnabled(false);
        }
    }

    public String getName() {
        return txf.getText();
    }

    public void insertUpdate(DocumentEvent theEvent) {
        changedUpdate(theEvent);
    }

    public void removeUpdate(DocumentEvent theEvent) {
        changedUpdate(theEvent);
    }

}
