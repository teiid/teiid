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

package com.metamatrix.console.ui.views.users;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.Collection;

import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;

/**
 * Extension to JDialog to show a list of check boxes, each for a principal
 * that may be added.
 */
public class AddDialog extends JDialog {
    private CheckBox[] items;
    private ButtonWidget okButton = new ButtonWidget("OK");
    private ButtonWidget cancelButton = new ButtonWidget("Cancel");

    public AddDialog(String title, String[] names) {
        super(ViewManager.getMainFrame(), title);
        init(names);
        okButton.setName("AddDialog.okButton");
        cancelButton.setName("AddDialog.cancelButton");
    }

    private void init(String[] names) {
        setModal(true);
        okButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                okPressed();
            }
        });
        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        items = new CheckBox[names.length];
        getRootPane().setDefaultButton(okButton);
        for (int i = 0; i < items.length; i++) {
            items[i] = new CheckBox(names[i]);
            items[i].addItemListener(new ItemListener() {
                public void itemStateChanged(ItemEvent ev) {
                    stateChanged();
                }
            });
        }
        int maxItem = maxCheckBoxItemLength();
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        int preferredWidth = Math.max(300, (maxItem * 6) + 160);
        int preferredHeight = 30 * items.length + 160;
        setSize(new Dimension(Math.min(preferredWidth, (int)(screenSize.width * .75)),
                Math.min(preferredHeight, (int)(screenSize.height * .75))));
        GridBagLayout layout = new GridBagLayout();
        getContentPane().setLayout(layout);
        JPanel listPanel = new JPanel();
        listPanel.setLayout(new GridLayout(items.length, 1));
        for (int i = 0; i < items.length; i++) {
            JPanel listItemPanel = new JPanel();
            GridBagLayout ll = new GridBagLayout();
            listItemPanel.setLayout(ll);
            listItemPanel.add(items[i]);
            ll.setConstraints(items[i], new GridBagConstraints(0, 0, 1, 1,
                    1.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                    new Insets(0, 5, 0, 0), 0, 0));
            listPanel.add(listItemPanel);
        }
        JScrollPane listJSP = new JScrollPane(listPanel);
        getContentPane().add(listJSP);
        layout.setConstraints(listJSP, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
        JPanel buttonPanel = new JPanel();
        GridBagLayout bl = new GridBagLayout();
        buttonPanel.setLayout(bl);
        JPanel innerButtonPanel = new JPanel();
        innerButtonPanel.setLayout(new GridLayout(1, 2, 5, 0));
        innerButtonPanel.add(okButton);
        innerButtonPanel.add(cancelButton);
        buttonPanel.add(innerButtonPanel);
        bl.setConstraints(innerButtonPanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        getContentPane().add(buttonPanel);
        layout.setConstraints(buttonPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 0), 0, 0));
        okButton.setEnabled(false);
        setLocation(StaticUtilities.centerFrame(getSize()));
    }

    private int maxCheckBoxItemLength() {
        int max = 0;
        for (int i = 0; i < items.length; i++) {
            int len = items[i].getText().length();
            if (len > max) {
                max = len;
            }
        }
        return max;
    }

    private void stateChanged() {
        okButton.setEnabled(anyItemChecked());
    }

    private boolean anyItemChecked() {
        boolean checked = false;
        int i = 0;
        while ((i < items.length) && (!checked)) {
            if (items[i].isSelected()) {
                checked = true;
            } else {
                i++;
            }
        }
        return checked;
    }

    private void cancelPressed() {
        //Set all items to unchecked, then dispose of dialog.
        for (int i = 0; i < items.length; i++) {
            items[i].setSelected(false);
        }
        dispose();
    }

    private void okPressed() {
        dispose();
    }

    public Collection /*<String>*/ getCheckedItems() {
        ArrayList list = new ArrayList();
        for (int i = 0; i < items.length; i++) {
            if (items[i].isSelected()) {
                list.add(items[i].getText());
            }
        }
        return list;
    }
}
