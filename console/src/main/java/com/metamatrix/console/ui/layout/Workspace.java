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

package com.metamatrix.console.ui.layout;

import java.awt.*;

import javax.swing.*;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.util.RepaintController;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.LabelWidget;

public class Workspace extends JPanel {
    private static Workspace theWorkspace = null;
    private WorkspacePanel currentPanel = null;
    private RepaintController repaintController;
    private LabelWidget lbl;
    private ConsoleMainFrame mainFrame;
    private GridBagConstraints gbc;

    private Workspace(RepaintController rc) {
        super();
        repaintController = rc;
        setLayout(new GridBagLayout());

        JPanel pnl = new JPanel();
        pnl.setBackground(PanelsTree.UNSELECTED_BACKGROUND_COLOR);
        pnl.setBorder(BorderFactory.createEtchedBorder());
        GridBagLayout pnlLayout = new GridBagLayout();
        pnl.setLayout(pnlLayout);
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1.0;
        gbc.weighty = 0.0;
        gbc.insets = new Insets(5, 5, 5, 5);
        add(pnl, gbc);

        lbl = new LabelWidget();
        Font font = lbl.getFont();
        font = font.deriveFont(Font.BOLD);
        lbl.setFont(font.deriveFont(font.getSize2D() * 1.5F));
        pnl.add(lbl);
        pnlLayout.setConstraints(lbl, new GridBagConstraints(0, 0, 1, 1,
        		0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
        		new Insets(3, 3, 3, 3), 0, 0));

        // setup constraints to use for changing the workspace panels
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1.0;
        gbc.insets = new Insets(5, 5, 5, 5);
    }

    public static void createInstance(RepaintController rc) {
        if (theWorkspace == null) {
            theWorkspace = new Workspace(rc);
        } else {
            String msg = "Attempt to create duplicate Workspace."; //$NON-NLS-1$
            LogManager.logError(LogContexts.INITIALIZATION, msg);
            throw new RuntimeException(msg);
        }
    }

    public static Workspace getInstance() {
        if (theWorkspace == null) {
            throw new IllegalStateException("Must first call createInstance."); //$NON-NLS-1$
        }
        return theWorkspace;
    }

    public void showPanel(WorkspacePanel panel) {
		if (currentPanel != null) {
            remove((JComponent)currentPanel);
        }
        currentPanel = panel;
        add((JComponent)currentPanel, gbc);
        lbl.setText(currentPanel.getTitle());
        repaintController.repaintNeeded();
        if (mainFrame == null) {
            mainFrame = ConsoleMainFrame.getInstance();
        }
        
        // Build up Frame title from parts
        String title = ConsolePlugin.Util.getString("ConsoleMainFrame.title"); //$NON-NLS-1$
        StringBuffer sb = new StringBuffer(title);
        sb.append(" - "); //$NON-NLS-1$
        sb.append(panel.getConnection().getURL()); 
        sb.append(" ["); //$NON-NLS-1$
        sb.append(panel.getConnection().getUser()); 
        sb.append("] - "); //$NON-NLS-1$
        sb.append(currentPanel.getTitle()); 
        
        mainFrame.setTitle(sb.toString());
	}

    public WorkspacePanel getCurrentPanel() {
        return currentPanel;
    }
}
