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

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JPanel;

import com.metamatrix.console.ui.layout.BasePanel;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public class EntitlementSelector extends BasePanel {
    private String title;
    private EntitlementsTable table;
    private ButtonWidget button = new ButtonWidget(" >> ");
    private TextFieldWidget entName = new TextFieldWidget();
    private TextFieldWidget vdbName = new TextFieldWidget();
    private TextFieldWidget vdbVers = new TextFieldWidget();
    private String entNameText;
    private String vdbNameText;
    private String vdbVersText;

    public EntitlementSelector(String ttl, EntitlementsTable tbl) {
        super();
        title = ttl;
        table = tbl;
        init();
    }

    private void init() {
        TitledBorder tBorder = new TitledBorder(title);
        this.setBorder(tBorder);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
        button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                buttonPressed();
            }
        });
        entName.setEditable(false);
        vdbName.setEditable(false);
        vdbVers.setEditable(false);
        LabelWidget entNameLabel = new LabelWidget("Role:");
        LabelWidget vdbNameLabel = new LabelWidget("VDB:");
        LabelWidget vdbVersLabel = new LabelWidget("vers:");
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        add(button);
        layout.setConstraints(button, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        JPanel listPanel = new JPanel();
        add(listPanel);
        layout.setConstraints(listPanel, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        GridBagLayout ll = new GridBagLayout();
        listPanel.setLayout(ll);
        listPanel.add(entNameLabel);
        ll.setConstraints(entNameLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        listPanel.add(vdbNameLabel);
        ll.setConstraints(vdbNameLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        listPanel.add(vdbVersLabel);
        ll.setConstraints(vdbVersLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        listPanel.add(entName);
        listPanel.add(vdbName);
        listPanel.add(vdbVers);
        ll.setConstraints(entName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(2, 2, 2, 2), 0, 0));
        ll.setConstraints(vdbName, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(2, 2, 2, 2), 0, 0));
        ll.setConstraints(vdbVers, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(2, 2, 2, 2), 0, 0));
        table.getSelectionModel().setSelectionInterval(0, 0);
        buttonPressed();
    }

    private void buttonPressed() {
        entNameText ="";
        vdbNameText = "";
        vdbVersText = "";
        int row = table.getSelectionModel().getLeadSelectionIndex();
        if (row >= 0) {
            entNameText = table.getModel().getValueAt(row, EntitlementsTableModel.ENTITLEMENT_COL_NUM).toString();
            vdbNameText = table.getModel().getValueAt(row, EntitlementsTableModel.VDB_COL_NUM).toString();
            Object versNum = table.getModel().getValueAt(row, EntitlementsTableModel.
                    VDB_VERS_COL_NUM);
            if (versNum != null) {
                vdbVersText = versNum.toString();
            }
        }
//        entName.setText("Ent.: " + entNameText);
//        vdbName.setText("VDB: " + vdbNameText);
//        vdbVers.setText("vers.: " + vdbVersText);
        entName.setText(entNameText);
        vdbName.setText(vdbNameText);
        vdbVers.setText(vdbVersText);
    }

    public EntitlementsTableRowData getSelection() {
        String entNameStr = entNameText;
        String vdbNameStr = "";
        int vdbVersion = -1;
        if (!(entNameStr.equals("") ||
                entNameStr.equals(""))) {
            vdbNameStr = vdbNameText;
            vdbVersion = (new Integer(vdbVersText)).intValue();
        }
        return new EntitlementsTableRowData(entNameStr, vdbNameStr, vdbVersion);
    }
}



