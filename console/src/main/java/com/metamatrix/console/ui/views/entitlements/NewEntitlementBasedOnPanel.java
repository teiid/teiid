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

package com.metamatrix.console.ui.views.entitlements;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class NewEntitlementBasedOnPanel extends BasicWizardSubpanelContainer {

    private EntitlementsTable table;
    private String entNameText;
    private String vdbNameText, vdbVersText;
    private Object versNum;
//    private boolean clear = false;
    private LabelWidget name = new LabelWidget("Role Name");
    private TextFieldWidget nameTW = new TextFieldWidget(10);
    private LabelWidget versionLW = new LabelWidget("Role Version");
    private TextFieldWidget versionTW = new TextFieldWidget();
    private LabelWidget versionNLW = new LabelWidget("Version Number");
    private TextFieldWidget versionNum = new TextFieldWidget();

    public NewEntitlementBasedOnPanel(int stepNum, String title, 
    		String[] paragraphs, 
    		EntitlementsTableModel model, WizardInterface wizardInterface) {
        super(wizardInterface);
        JPanel panel = init(model);
        this.setMainContent(panel);
        this.setStepText(stepNum, true, title, paragraphs);
    }

	private JPanel init(EntitlementsTableModel oldModel) {
        JPanel panel = new JPanel();
        ButtonWidget clearButton = new ButtonWidget("Clear");
        clearButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                table.getSelectionModel().clearSelection();
//                clear = true;
                versionNum.setText("");
                nameTW.setText("");
                versionTW.setText("");
            }
        });
        GridBagLayout layout = new GridBagLayout();
        panel.setLayout(layout);
        EntitlementsTableRowData[] rowData = convertToRowData(oldModel);
        EntitlementsTableModel newModel = new EntitlementsTableModel(rowData);
        table = new EntitlementsTable(newModel);
        table.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent ev) {
                tableSelectionChanged();
            }
        });
        JScrollPane tableSP = new JScrollPane(table);
        panel.add(tableSP);
        layout.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
       /* authorizationsSelector = new EntitlementSelector(
                "Set authorizations from entitlement", table);
        panel.add(authorizationsSelector);
        layout.setConstraints(authorizationsSelector, new GridBagConstraints(
                1, 0, 1, 1, 0.5, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
        principalsSelector = new EntitlementSelector(
                "Set principals from entitlement", table);
        panel.add(principalsSelector);  */
        panel.add(clearButton);
        layout.setConstraints(clearButton, new GridBagConstraints(
                0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        JPanel detailPanel = new JPanel();
        GridBagLayout dpLayout = new GridBagLayout();
        detailPanel.setLayout(dpLayout);
        
        dpLayout.setConstraints(name, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        dpLayout.setConstraints(nameTW, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        dpLayout.setConstraints(versionLW, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        dpLayout.setConstraints(versionTW, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        dpLayout.setConstraints(versionNLW, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        dpLayout.setConstraints(versionNum, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        nameTW.setEditable(false);
        versionTW.setEditable(false);
        versionNum.setEditable(false);
        detailPanel.add(name);
        detailPanel.add(nameTW);
        detailPanel.add(versionLW);
        detailPanel.add(versionTW);
        detailPanel.add(versionNLW);
        detailPanel.add(versionNum);
        layout.setConstraints(detailPanel, new GridBagConstraints(
                0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.SOUTH,
                GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
        panel.add(detailPanel);
        return panel;
    }

    private void tableSelectionChanged(){

        int apparentRow = table.getSelectionModel().getLeadSelectionIndex();

        int row = table.convertRowIndexToModel( apparentRow );
        if (row >= 0) {
            String enText = table.getModel().getValueAt(row, EntitlementsTableModel.ENTITLEMENT_COL_NUM).toString();
            String vnText = table.getModel().getValueAt(row, EntitlementsTableModel.VDB_COL_NUM).toString();
            Object vNum = table.getModel().getValueAt(row, EntitlementsTableModel.
                    VDB_VERS_COL_NUM);
            if (vNum != null) {
                String vvText = vNum.toString().trim();
                versionNum.setText(vvText);
            }
            nameTW.setText(enText);
            versionTW.setText(vnText);
        }

    }
    private EntitlementsTableRowData[] convertToRowData(EntitlementsTableModel model) {
        int modelRowCount = model.getRowCount();
        EntitlementsTableRowData[] rows = new EntitlementsTableRowData[modelRowCount];
        //rows[0] = new EntitlementsTableRowData(NONE, "", -1);
        for (int i = 0; i < modelRowCount; i++) {
            rows[i] = new EntitlementsTableRowData(model.getValueAt(i,
                    EntitlementsTableModel.ENTITLEMENT_COL_NUM).toString(), model.
                    getValueAt(i, EntitlementsTableModel.VDB_COL_NUM).toString(),
                    ((Integer)model.getValueAt(i, EntitlementsTableModel.VDB_VERS_COL_NUM))
                    .intValue());
        }
        return rows;
    }
  /*
    public String getDataNodesSelection() {
        return makeEntitlementString(authorizationsSelector.getSelection());
    }      */

    public EntitlementsTableRowData getDataNodesSelectionRowData() {
        if (table.getSelectionModel().isSelectionEmpty()){
            return new EntitlementsTableRowData(null, null, -1);
        }
        int apparentRow = table.getSelectionModel().getLeadSelectionIndex();
        int row = table.convertRowIndexToModel( apparentRow );
        if (row >= 0) {
            entNameText = table.getModel().getValueAt(row, EntitlementsTableModel.ENTITLEMENT_COL_NUM).toString();
            vdbNameText = table.getModel().getValueAt(row, EntitlementsTableModel.VDB_COL_NUM).toString();
            versNum = table.getModel().getValueAt(row, EntitlementsTableModel.
                    VDB_VERS_COL_NUM);
            if (versNum != null) {
                vdbVersText = versNum.toString().trim();
                versionNum.setText(vdbVersText);
            }
            nameTW.setText(entNameText);
            versionTW.setText(vdbNameText);
        }

        return getSelection();
    }

    public EntitlementsTableRowData getSelection() {
        String entNameStr = entNameText;
        String vdbNameStr = "";
        int vdbVersion = -1;

            vdbNameStr = vdbNameText;
            if (vdbVersText != null){
            vdbVersion = (new Integer(vdbVersText)).intValue();
            }

        return new EntitlementsTableRowData(entNameStr, vdbNameStr, vdbVersion);
    }

}//end NewEntitlementBasedOnPanel
