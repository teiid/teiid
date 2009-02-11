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
import java.awt.Insets;

import javax.swing.AbstractButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;

import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.StaticUtilities;

public class VDBWizardModelVisibilityPanel extends BasicWizardSubpanelContainer {
    private NewVDBWizardModelVisibilityTable table;
    private boolean[] currentVisibilityCheckBoxesState = null;
    private boolean tablePopulated = false;
    private ModelVisibilityInfo[] infoPopulatedFrom = null;

    private int panelOrder;
 
    public VDBWizardModelVisibilityPanel(int step, WizardInterface wizardInterface) {
        super(wizardInterface);
        panelOrder = step;
        JPanel panel = init();
        super.setMainContent(panel);
        String[] paragraphs = new String[] {
        		"Check or uncheck the \"Visible\" column to set the visibility for each " +
                "model.  Only models checked as \"Visible\"" +
                "can be used in queries.  Check or uncheck the \"Multiple Source\" column " +
                "to indicate whether or not a model may have multiple data sources.",
                "Note: Visibility and multiple-source availability cannot be changed " +
                "for a model once a Virtual Database has been created."};
        super.setStepText(panelOrder, true, "Set Model Properties",
        		paragraphs);
        table.getModel().addTableModelListener(new TableModelListener() {
            public void tableChanged(TableModelEvent ev) {
                tableWasChanged();
            }
        });
    }

    private JPanel init() {
        JPanel panel = new JPanel();
        table = new NewVDBWizardModelVisibilityTable();
        JScrollPane scrollPane = new JScrollPane(table);
        GridBagLayout layout = new GridBagLayout();
        panel.setLayout(layout);
        panel.add(scrollPane);
        layout.setConstraints(scrollPane, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 4, 4, 4), 0, 0));
        return panel;
    }

    public void populateTable(ModelVisibilityInfo[] info) {
        table.populateTable(info);
        tablePopulated = true;
        infoPopulatedFrom = info;
    }

    public boolean modelsMatch(ModelVisibilityInfo[] info) {
        boolean match = false;
        if (infoPopulatedFrom != null) {
            if (info.length == infoPopulatedFrom.length) {
                boolean mismatchFound = false;
                int i = 0;
                while ((i < info.length) && (!mismatchFound)) {
                    if (!info[i].getModelName().equals(
                            infoPopulatedFrom[i].getModelName())) {
                        mismatchFound = true;
                    } else if (!info[i].getModelType().equals(
                            infoPopulatedFrom[i].getModelType())) {
                        mismatchFound = true;
//                    } else if (info[i].getModelVersion() !=
//                            infoPopulatedFrom[i].getModelVersion()) {
//                        mismatchFound = true;
                    }
                    i++;
                }
                if (!mismatchFound) {
                    match = true;
                }
            }
        }
        return match;
    }

    public boolean isTablePopulated() {
        return tablePopulated;
    }

    public ModelVisibilityInfo[] getUpdatedVisibilityInfo() {
        return table.getUpdatedVisibilityInfo();
    }

    public void tableWasChanged() {
        if (currentVisibilityCheckBoxesState != null) {
            boolean[] newVisibilityCheckBoxesState = visibilityState();
            if (currentVisibilityCheckBoxesState.length !=
                    newVisibilityCheckBoxesState.length) {
                currentVisibilityCheckBoxesState =
                        newVisibilityCheckBoxesState;
            } else {
                if (!visibilityMatchesCurrent(newVisibilityCheckBoxesState)) {
                    AbstractButton forwardButton = getWizardInterface().getForwardButton();
                    boolean nextEnabled = forwardButton.isEnabled();
                    boolean shouldBeEnabled = table.anyPublic();
                    if (shouldBeEnabled != nextEnabled) {
                        forwardButton.setEnabled(shouldBeEnabled);
                        if (!shouldBeEnabled) {
                            StaticUtilities.displayModalDialogWithOK(
                                    getWizardInterface().getOwner(),
                                    "Visible Model Required",
                                    "Must check at least one model as Visible.");
                        }
                    }
                    currentVisibilityCheckBoxesState = newVisibilityCheckBoxesState;
                }
            }
        } else {
            currentVisibilityCheckBoxesState = visibilityState();
        }
    }

    private boolean[] visibilityState() {
        boolean[] visible = null;
        if (table != null) {
            int numRows = table.getRowCount();
            visible = new boolean[numRows];
            for (int i = 0; i < numRows; i++) {
                visible[i] = table.publicCheckedForRow(i);
            }
        }
        return visible;
    }

    private boolean visibilityMatchesCurrent(boolean[] newVisibility) {
        boolean mismatchFound = false;
        int i = 0;
        while ((i < newVisibility.length) && (!mismatchFound)) {
            if (currentVisibilityCheckBoxesState[i] != newVisibility[i]) {
                mismatchFound = true;
            } else {
                i++;
            }
        }
        return (!mismatchFound);
    }
}
