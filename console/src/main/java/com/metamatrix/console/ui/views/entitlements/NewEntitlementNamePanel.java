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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;

import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.TypeConstants;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.StaticQuickSorter;

import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

public class NewEntitlementNamePanel extends BasicWizardSubpanelContainer
        implements TypeConstants{
    private final static double LABELS_X_WEIGHT = 0.3;
    private final static double VALUES_X_WEIGHT = 0.7;
    public final static int MAX_DESCRIPTION_LENGTH = 250;

    private TextFieldWidget nameField = GuiComponentFactory.createTextField(ENTITLEMENT_NAME);
    private JTextArea descriptionArea;
    private JScrollPane scpnDescription = new JScrollPane();
    private JComboBox vdbNameBox = new JComboBox();
    private JComboBox vdbVersionBox = new JComboBox();
    private EntitlementsDataInterface dataSource;
//    private NewEntitlementController controller;
    private boolean hasBeenPainted = false;

    public NewEntitlementNamePanel(EntitlementsDataInterface dataSrc,
            NewEntitlementController ctrlr, WizardInterface wizardInterface)
            throws ExternalException,
            AuthorizationException, ComponentNotFoundException {
        super(wizardInterface);
        dataSource = dataSrc;
//        controller = ctrlr;
        JPanel panel = init();
        this.setMainContent(panel);
        this.setStepText(1, "Enter Role name and VDB information.");
    }

    private JPanel init() throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        JPanel panel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        panel.setLayout(layout);
        vdbNameBox.setEditable(false);
        vdbVersionBox.setEditable(false);
        JPanel entryPanel = new JPanel();
        panel.add(entryPanel);
        layout.setConstraints(entryPanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(0, 20, 0, 0), 0, 0));
        GridBagLayout el = new GridBagLayout();
        entryPanel.setLayout(el);

        DefaultTextFieldModel document = new DefaultTextFieldModel();
        document.setMaximumLength(MAX_DESCRIPTION_LENGTH);
        descriptionArea = new JTextArea(document);
        descriptionArea.setColumns(30);
        descriptionArea.setRows(6);
        descriptionArea.setPreferredSize(new Dimension(150, 68));
        descriptionArea.setLineWrap(true);
        descriptionArea.setWrapStyleWord(true);

        descriptionArea.setText("");
        scpnDescription.setViewportView(descriptionArea);

        LabelWidget nameLabel = new LabelWidget("*Role name:");
        setBoldFont(nameLabel);
        entryPanel.add(nameLabel);
        el.setConstraints(nameLabel, new GridBagConstraints(0, 0, 1, 1,
                LABELS_X_WEIGHT, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        nameField.addKeyListener(new KeyAdapter() {
            public void keyReleased(KeyEvent ev) {
                checkOnEnabling(false);
            }
            public void keyTyped(KeyEvent ev) {
                checkOnEnabling(false);
            }
        });
        try {
            nameField.setInvalidCharacters(new String(new byte[] {
                    AuthorizationPolicyID.DELIMITER}));
        } catch (ParseException ex) {
            //Cannot happen
        }
        entryPanel.add(nameField);
        el.setConstraints(nameField, new GridBagConstraints(1, 0, 1, 1,
                VALUES_X_WEIGHT, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget descriptionLabel = new LabelWidget("Role description:");
        entryPanel.add(descriptionLabel);
        el.setConstraints(descriptionLabel, new GridBagConstraints(0, 1,
                1, 1, LABELS_X_WEIGHT, 0.0, GridBagConstraints.EAST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        entryPanel.add(descriptionArea);
        el.setConstraints(descriptionArea, new GridBagConstraints(1, 1,
                1, 1, VALUES_X_WEIGHT, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget vdbNameLabel = new LabelWidget("*VDB name:");
        setBoldFont(vdbNameLabel);
        entryPanel.add(vdbNameLabel);
        el.setConstraints(vdbNameLabel, new GridBagConstraints(0, 2, 1, 1,
                LABELS_X_WEIGHT, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        Collection /*<VirtualDatabase>*/ vdbsColl = dataSource.getAllVDBs();
        String[] unsortedVDBs = new String[vdbsColl.size() + 1];
        unsortedVDBs[0] = "                    ";
        Iterator it = vdbsColl.iterator();
        for (int i = 1; it.hasNext(); i++) {
            VirtualDatabase vdb = (VirtualDatabase)it.next();
            unsortedVDBs[i] = vdb.getName();
        }
        String[] vdbs = StaticQuickSorter.quickStringSort(unsortedVDBs);
        vdbs = removeDuplicates(vdbs);
        vdbNameBox.addItemListener(new ItemListener() {
            public void itemStateChanged(ItemEvent ev) {
                checkOnEnabling(true);
            }
        });
        vdbNameBox.setModel(new DefaultComboBoxModel(vdbs));
        entryPanel.add(vdbNameBox);
        el.setConstraints(vdbNameBox, new GridBagConstraints(1, 2, 1, 1,
                VALUES_X_WEIGHT, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget vdbVersionLabel = new LabelWidget("*VDB Version:");
        setBoldFont(vdbVersionLabel);
        entryPanel.add(vdbVersionLabel);
        el.setConstraints(vdbVersionLabel, new GridBagConstraints(0, 3, 1, 1,
                LABELS_X_WEIGHT, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        vdbVersionBox.addItemListener(new ItemListener() {
            public void itemStateChanged(ItemEvent ev) {
                checkOnEnabling(false);
            }
        });
        vdbVersionBox.setEnabled(false);
        entryPanel.add(vdbVersionBox);
        el.setConstraints(vdbVersionBox, new GridBagConstraints(1, 3, 1, 1,
                VALUES_X_WEIGHT, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        LabelWidget requiredFieldLabel = new LabelWidget("*Required field");
        setBoldFont(requiredFieldLabel);
        entryPanel.add(requiredFieldLabel);
        el.setConstraints(requiredFieldLabel, new GridBagConstraints(0, 4, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(15, 5, 5, 5), 0, 0));

        nameField.requestFocus();
        return panel;
    }

    public void postRealize() {
        nameField.requestFocus();
    }

    public void checkOnEnabling(boolean vdbNameSelectionChanged) {
        String name = nameField.getText().trim();
        boolean nameEntered = (name.length() > 0);
        String vdb = ((String)vdbNameBox.getSelectedItem()).trim();
        boolean vdbSelected = (vdb.length() > 0);
        vdbVersionBox.setEnabled(vdbSelected);
        Vector versions = null;
        if (vdbNameSelectionChanged) {
            if (vdbSelected) {
                try {
                    int[] versionsArray = getVersionsForVDB(vdb);
                    versions = new Vector(versionsArray.length);
                    for (int i = 0; i < versionsArray.length; i++) {
                        versions.add(new Integer(versionsArray[i]));
                    }
                } catch (Exception ex) {
                    ExceptionUtility.showMessage("Retrieve versions for selected VDB", ex);
                    return;
                }
            } else {
                versions = new Vector(1);
                versions.add("");
            }
            vdbVersionBox.setModel(new DefaultComboBoxModel(versions));
            vdbVersionBox.setSelectedIndex(versions.size() - 1);
        }
        AbstractButton nextButton = getWizardInterface().getForwardButton();
        nextButton.setEnabled((nameEntered && vdbSelected));
    }

    private int[] getVersionsForVDB(String vdb) throws
            AuthorizationException, ExternalException,
            ComponentNotFoundException {
        return dataSource.getVersionsForVDB(vdb);
    }

    public String getEntitlementName() {
        return nameField.getText().trim();
    }

    public String getEntitlementDescription() {
        return descriptionArea.getText().trim();
    }

    public String getVDBName() {
        return (String)vdbNameBox.getSelectedItem();
    }

    public int getVDBVersion() {
        return ((Integer)vdbVersionBox.getSelectedItem()).intValue();
    }

    private String[] removeDuplicates(String[] sortedStr) {
        if (sortedStr.length == 0) {
            return sortedStr;
        }
        Collection c = new ArrayList(sortedStr.length);
        c.add(sortedStr[0]);
        for (int i = 1; i < sortedStr.length; i++) {
            if (!sortedStr[i].equals(sortedStr[i - 1])) {
                c.add(sortedStr[i]);
            }
        }
        String[] withoutDuplicates = new String[c.size()];
        Iterator it = c.iterator();
        for (int i = 0; it.hasNext(); i++) {
            withoutDuplicates[i] = (String)it.next();
        }
        return withoutDuplicates;
    }

    private void setBoldFont(LabelWidget label) {
        Font tempFont = label.getFont();
        Font newFont = new Font(tempFont.getName(), Font.BOLD, tempFont.getSize());
        label.setFont(newFont);
    }
    
    public void paint(Graphics g) {
        if (!hasBeenPainted) {
            AbstractButton nextButton = getWizardInterface().getForwardButton();
            nextButton.setEnabled(false);
            hasBeenPainted = true;
        }
        super.paint(g);
    }
}
