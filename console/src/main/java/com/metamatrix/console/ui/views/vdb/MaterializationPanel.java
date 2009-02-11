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

package com.metamatrix.console.ui.views.vdb;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collection;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import com.metamatrix.admin.api.objects.AdminObject;
import com.metamatrix.admin.api.objects.VDB;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/**
 * Panel displayed as one of the tabs at the bottom of VdbMainPanel.
 * Used for saving Materialization scripts for the VDB.
 * @since 4.3
 */
public class MaterializationPanel extends JPanel implements
                                            VdbDisplayer {

    private final static String SAVE_BTN_LABEL = ConsolePlugin.Util.getString("MaterializationPanel.save"); //$NON-NLS-1$
    private final static String NO_MATERIALIZATION_TEXT = ConsolePlugin.Util.getString("MaterializationPanel.noMaterialization"); //$NON-NLS-1$
    private final static String UNEXPECTED_ERROR = ConsolePlugin.Util.getString("MaterializationPanel.unexpectedError"); //$NON-NLS-1$ 
    private final static String UNEXPECTED_NUMBER_OF_VDBS = "MaterializationPanel.unexpectedNumberOfVDBs"; //$NON-NLS-1$ 

    
    private VirtualDatabase virtualDatabase;
    private JButton saveButton;
    private JPanel buttonsPanel;
    private ConnectionInfo connection;

    public MaterializationPanel(ConnectionInfo connection) {
        super();
        this.connection = connection;
        createButtons();
    }


    private void createButtons() {
        saveButton = new ButtonWidget(SAVE_BTN_LABEL);
        saveButton.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent ev) {
                savePressed();
            }
        });

        buttonsPanel = new JPanel(new GridLayout(1, 2, 5, 0));
        buttonsPanel.add(saveButton);
    }

    private void savePressed() {    
        
        VirtualDatabaseID id = (VirtualDatabaseID)virtualDatabase.getID();
        String version = id.getVersion();
        MaterializationWizardDialog dialog = 
            new MaterializationWizardDialog(ConsoleMainFrame.getInstance(), virtualDatabase.getName(), version, connection);
        dialog.show();
    }

    public void setVirtualDatabase(VirtualDatabase virtualDatabase) {
        if (this.virtualDatabase != virtualDatabase) {
            this.virtualDatabase = virtualDatabase;

            this.removeAll();
            if (virtualDatabase != null) {
                GridBagLayout layout = new GridBagLayout();
                setLayout(layout);
                
                if (hasMaterializedViews(virtualDatabase)) {
                    JPanel spacerPanel = new JPanel();
                    add(spacerPanel);
                    layout.setConstraints(spacerPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, 
                                                                              GridBagConstraints.CENTER,
                                                                              GridBagConstraints.BOTH, 
                                                                              new Insets(4, 4, 4, 4), 0, 0));
                    add(buttonsPanel);
                    layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, 
                                                                               GridBagConstraints.CENTER,
                                                                               GridBagConstraints.NONE, 
                                                                               new Insets(4, 4, 4, 4), 0, 0));
                    
                } else {
                    JTextArea noMaterializationTextArea = new JTextArea(NO_MATERIALIZATION_TEXT);
                    noMaterializationTextArea.setEditable(false);
                    noMaterializationTextArea.setLineWrap(true);
                    noMaterializationTextArea.setWrapStyleWord(true);
                    noMaterializationTextArea.setBackground(this.getBackground());
                    this.add(noMaterializationTextArea);
                    layout.setConstraints(noMaterializationTextArea, 
                                          new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, 
                                                                 GridBagConstraints.CENTER,
                                                                 GridBagConstraints.BOTH,
                                                                 new Insets(10, 10, 10, 10), 0, 0));
                    
                }
                
            }
        }
    }

    
    private boolean hasMaterializedViews(VirtualDatabase virtualDatabase) {
        ServerAdmin admin = null;
        try {
            admin = connection.getServerAdmin();
            String identifier = virtualDatabase.getName() + AdminObject.DELIMITER_CHAR + virtualDatabase.getVirtualDatabaseID().getVersion();
            Collection vdbs = admin.getVDBs(identifier);
            if (vdbs == null || vdbs.size() != 1) {
                String message = ConsolePlugin.Util.getString(UNEXPECTED_NUMBER_OF_VDBS, identifier, 
                                                              Integer.toString(vdbs == null ? 0 : vdbs.size()));
                throw new Exception(message);
            }
            VDB vdb = (VDB) vdbs.iterator().next();
            return vdb.hasMaterializedViews();
        } catch (Exception e) {
            LogManager.logError(LogContexts.VIRTUAL_DATABASE, e, UNEXPECTED_ERROR);
            ExceptionUtility.showMessage(UNEXPECTED_ERROR, e);
            return false;
        } 
    }
}
