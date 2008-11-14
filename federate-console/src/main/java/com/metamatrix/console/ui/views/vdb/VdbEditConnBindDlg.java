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

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.swing.JDialog;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.VdbManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.toolbox.ui.widget.DialogPanel;

public class VdbEditConnBindDlg extends JDialog {

    private VdbAssignConnBindPanel vacbConnectorBindingEditPanel;
    private VirtualDatabaseID vdbID;
    private ConnectionInfo connection = null;
    private DialogPanel dlgpnlDialogContainer = new DialogPanel();

    public VdbEditConnBindDlg(Frame mainFrame,
                              String title,
                              VirtualDatabaseID vdbID,
                              ConnectionInfo connection) {
        super(mainFrame, title);
        this.vdbID = vdbID;
        this.connection = connection;
        try {
            init();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private VdbManager getVdbManager() {
        return ModelManager.getVdbManager(connection);
    }

    private void init() throws Exception {
        vacbConnectorBindingEditPanel = new VdbAssignConnBindPanel(connection);

        Collection colModels = getModelsForVdb(getVdbId());

        vacbConnectorBindingEditPanel.setModels(convertModelsToModelWrappers(colModels));

        dlgpnlDialogContainer.setContent(vacbConnectorBindingEditPanel);
        addListeners();

        getContentPane().add(dlgpnlDialogContainer);
    }

    public Collection convertModelsToModelWrappers(Collection colModels) {

        // convert these models into modelwrappers

        ArrayList arylModelWrappers = new ArrayList();
        Iterator it = colModels.iterator();

        while (it.hasNext()) {
            Model mdlTemp = (Model)it.next();
            // add only models that require a binding
            if (mdlTemp.requireConnectorBinding()) {
                arylModelWrappers.add(new ModelWrapper(mdlTemp));
            }
        }
        return arylModelWrappers;
    }

    private void addListeners() {
        dlgpnlDialogContainer.getAcceptButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processAcceptButton();
            }
        });

        dlgpnlDialogContainer.getCancelButton().addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                processCancelButton();
            }
        });
    }

    private void processAcceptButton() {
        updateConnectorBindings();
        dispose();
    }

    private void processCancelButton() {
        dispose();
    }

    private void updateConnectorBindings() {
        // verify map
        Map mapModelsToConnBinds = getModelsToConnBindsMap();
        try {
            getVdbManager().setConnectorBindingNames(getVdbId(), mapModelsToConnBinds);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed to update Connector Binding names ", e);
        }
    }

    private VirtualDatabaseID getVdbId() {
        return vdbID;
    }

    private Map /* <String model name to Collection of String UUID> */getModelsToConnBindsMap() {
        return vacbConnectorBindingEditPanel.getModelsToConnectorBindingsMap();
    }

    private Collection getModelsForVdb(VirtualDatabaseID vdbID) {
        Collection colModels = null;
        try {
            colModels = getVdbManager().getVdbModels(vdbID);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed retrieving models for a vdb", e);
        }
        return colModels;
    }
}
