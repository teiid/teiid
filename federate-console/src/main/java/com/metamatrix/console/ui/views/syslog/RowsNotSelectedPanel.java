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

package com.metamatrix.console.ui.views.syslog;

import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;

public class RowsNotSelectedPanel extends DialogPanel {
    private static int displayedPanelCount = 0;

    public static void incrementDisplayedPanelCount() {
        displayedPanelCount += 1;
    }

    public static void decrementDisplayedPanelCount() {
        displayedPanelCount -= 1;
    }

    public static boolean isCurrentlyDisplayed() {
        return (displayedPanelCount > 0);
    }

    public RowsNotSelectedPanel(int theCount, int maxRecords) {
        super();
        JPanel pnl = new JPanel(new GridBagLayout());
        pnl.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        JLabel lbl = new LabelWidget();
        if (theCount == 0) {
            lbl.setText(SysLogUtils.getString("msg.norowsselected"));
        } else {
            // too many rows would be selected
            lbl.setText(SysLogUtils.getString("msg.toomanyrowsselected",
                    new Object[] {"" + theCount, "" + maxRecords}));
        }
        lbl.setIcon(SysLogUtils.getIcon("icon.info"));
        pnl.add(lbl);
        setContent(pnl);
        ButtonWidget btnOk = getAcceptButton();
        btnOk.setText(SysLogUtils.getString("btnOk"));
        btnOk.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                okPressed();
            }
        });
        removeNavigationButton(getCancelButton());
    }

    protected void cancel(final WidgetActionEvent event) {
        exitingPanel();
    }

    private void okPressed() {
        exitingPanel();
    }

    private void exitingPanel() {
        RowsNotSelectedPanel.decrementDisplayedPanelCount();
    }
}
