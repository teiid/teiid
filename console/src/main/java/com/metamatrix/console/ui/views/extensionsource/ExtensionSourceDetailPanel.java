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

package com.metamatrix.console.ui.views.extensionsource;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;

import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.CenteredOptionPane;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

public class ExtensionSourceDetailPanel extends JPanel {
    private ExtensionSourceDetailListener listener;
    private boolean canModify;
    private int maxDescriptionLength;
    private String moduleName;
    private String moduleType;
    private String description;
//    private boolean enabled;
    private String creator;
    private Date creationDate;
    private String lastUpdater;
    private Date lastUpdateDate;
    private TextFieldWidget moduleNameTFW;
    private TextFieldWidget moduleTypeTFW;
    private JTextArea descriptionJTA;
    private TextFieldWidget creatorTFW;
    private TextFieldWidget creationDateTFW;
    private TextFieldWidget updaterTFW;
    private TextFieldWidget updateDateTFW;
    private ButtonWidget applyButton;
    private ButtonWidget resetButton;
    private ButtonWidget deleteButton;
    private ButtonWidget replaceButton;
    private ButtonWidget exportButton;
//    private JCheckBox enabledCB;
    private boolean settingInitialContents = false;
    private ExtensionSourceDetailInfo detailInfo = null;
    
    public ExtensionSourceDetailPanel(ExtensionSourceDetailListener lsnr,
            boolean modifiable, int maxDescLen) {
        super();
        listener = lsnr;
        canModify = modifiable;
        maxDescriptionLength = maxDescLen;
        init();
    }

	private void init() {
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        LabelWidget moduleNameLW = new LabelWidget("Module Name:");
        LabelWidget moduleTypeLW = new LabelWidget("Module Type:");
        LabelWidget createdLW = new LabelWidget("Created");
        LabelWidget createdByLW = new LabelWidget("by");
        LabelWidget lastUpdatedLW = new LabelWidget("Last Updated");
        LabelWidget lastUpdatedByLW = new LabelWidget("by");
        LabelWidget descriptionLW = new LabelWidget("Description:");
//        enabledCB = new CheckBox("Enabled");
        moduleNameTFW = new TextFieldWidget();
        moduleTypeTFW = new TextFieldWidget();
        DefaultTextFieldModel document = new DefaultTextFieldModel();
        document.setMaximumLength(maxDescriptionLength);
        descriptionJTA = new JTextArea(document);
        descriptionJTA.setRows(5);
        descriptionJTA.setColumns(35);
        descriptionJTA.setLineWrap(true);
        descriptionJTA.setWrapStyleWord(true);
        creatorTFW = new TextFieldWidget();
        creationDateTFW = new TextFieldWidget();
        updaterTFW = new TextFieldWidget();
        updateDateTFW = new TextFieldWidget();

//        enabledCB.setEnabled(canModify);
//        enabledCB.addItemListener(new ItemListener() {
//            public void itemStateChanged(ItemEvent ev) {
//                checkForChanges();
//            }
//        });

        moduleNameTFW.setEditable(canModify);
        moduleNameTFW.getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                checkForChanges();
            }
            public void removeUpdate(DocumentEvent ev) {
                checkForChanges();
            }
            public void insertUpdate(DocumentEvent ev) {
                checkForChanges();
            }
        });
        descriptionJTA.setEditable(canModify);
        descriptionJTA.getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                checkForChanges();
            }
            public void removeUpdate(DocumentEvent ev) {
                checkForChanges();
            }
            public void insertUpdate(DocumentEvent ev) {
                checkForChanges();
            }
        });
        moduleTypeTFW.setEditable(false);
        creatorTFW.setEditable(false);
        creationDateTFW.setEditable(false);
        updaterTFW.setEditable(false);
        updateDateTFW.setEditable(false);

        JPanel buttonsPanel = new JPanel();

        exportButton = new ButtonWidget("Export...");
        exportButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                exportPressed();
            }
        });
        exportButton.setEnabled(false);
        if (canModify) {
            applyButton = new ButtonWidget("Apply");
            applyButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    applyPressed();
                }
            });
            applyButton.setEnabled(false);
            resetButton = new ButtonWidget("Reset");
            resetButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    resetPressed();
                }
            });
            resetButton.setEnabled(false);
            replaceButton = new ButtonWidget("Replace...");
            replaceButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    replacePressed();
                }
            });
            replaceButton.setEnabled(false);
            deleteButton = new ButtonWidget("Delete");
            deleteButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    deletePressed();
                }
            });
            deleteButton.setEnabled(false);
            buttonsPanel.setLayout(new GridLayout(1, 5, 18, 0));
            buttonsPanel.add(applyButton);
            buttonsPanel.add(resetButton);
            buttonsPanel.add(replaceButton);
            buttonsPanel.add(exportButton);
            buttonsPanel.add(deleteButton);
        } else {
            buttonsPanel.setLayout(new GridLayout(1, 1));
            buttonsPanel.add(exportButton);
        }

        JPanel namePanel = new JPanel();
        GridBagLayout nameLayout = new GridBagLayout();
        namePanel.setLayout(nameLayout);
        namePanel.add(moduleNameLW);
        namePanel.add(moduleNameTFW);
        namePanel.add(moduleTypeLW);
        namePanel.add(moduleTypeTFW);
        nameLayout.setConstraints(moduleNameLW, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 0), 0, 0));
        nameLayout.setConstraints(moduleNameTFW, new GridBagConstraints(1, 0, 1, 1,
                2.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(0, 8, 0, 8), 0, 0));
        nameLayout.setConstraints(moduleTypeLW, new GridBagConstraints(2, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 0), 0, 0));
        nameLayout.setConstraints(moduleTypeTFW, new GridBagConstraints(3, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(0, 8, 0, 0), 0, 0));

        JPanel datesPanel = new JPanel();
        GridBagLayout datesLayout = new GridBagLayout();
        datesPanel.setLayout(datesLayout);
        datesPanel.add(createdLW);
        datesPanel.add(creationDateTFW);
        datesPanel.add(createdByLW);
        datesPanel.add(creatorTFW);
        datesPanel.add(lastUpdatedLW);
        datesPanel.add(updateDateTFW);
        datesPanel.add(lastUpdatedByLW);
        datesPanel.add(updaterTFW);
        datesLayout.setConstraints(createdLW, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 4, 4), 0, 0));
        datesLayout.setConstraints(creationDateTFW, new GridBagConstraints(
                1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.HORIZONTAL, new Insets(0, 4, 4, 8), 0, 0));
        datesLayout.setConstraints(createdByLW, new GridBagConstraints(2, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 8, 4, 4), 0, 0));
        datesLayout.setConstraints(creatorTFW, new GridBagConstraints(3, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(0, 4, 4, 0), 0, 0));
        datesLayout.setConstraints(lastUpdatedLW, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(4, 0, 4, 4), 0, 0));
        datesLayout.setConstraints(updateDateTFW, new GridBagConstraints(
                1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.HORIZONTAL, new Insets(4, 4, 4, 8), 0, 0));
        datesLayout.setConstraints(lastUpdatedByLW, new GridBagConstraints(2, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(4, 8, 4, 4), 0, 0));
        datesLayout.setConstraints(updaterTFW, new GridBagConstraints(3, 1, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 4, 4, 0), 0, 0));

        JPanel descPanel = new JPanel();
        GridBagLayout descLayout = new GridBagLayout();
        descPanel.setLayout(descLayout);
        descPanel.add(descriptionLW);
        descPanel.add(descriptionJTA);
        descLayout.setConstraints(descriptionLW, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 0), 0, 0));
        descLayout.setConstraints(descriptionJTA, new GridBagConstraints(1, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.VERTICAL,
                new Insets(0, 4, 0, 0), 0, 0));

        this.add(namePanel);
        this.add(datesPanel);
        this.add(descPanel);
//        this.add(enabledCB);
        this.add(buttonsPanel);
        layout.setConstraints(namePanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.5, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 10, 4, 10), 0, 0));
        layout.setConstraints(datesPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.5, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 10, 4, 10), 0, 0));
        layout.setConstraints(descPanel, new GridBagConstraints(0, 2, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 10, 4, 10), 0, 0));
//        layout.setConstraints(enabledCB, new GridBagConstraints(0, 3, 1, 1,
//                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
//                new Insets(4, 10, 4, 4), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 4, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(4, 10, 8, 10), 0, 0));
    }

    public void setInfo(ExtensionSourceDetailInfo info) {
    	detailInfo = info;
        if (info == null) {
            moduleName = "";
            moduleType = "";
            description = "";
            creator = "";
            creationDate = null;
            lastUpdater = "";
            lastUpdateDate = null;
 //           enabled = false;
            if (deleteButton != null) {
                deleteButton.setEnabled(false);
            }
            if (replaceButton != null) {
                replaceButton.setEnabled(false);
            }
            if (exportButton != null) {
                exportButton.setEnabled(false);
            }
        } else {
            moduleName = info.getModuleName();
            moduleType = info.getModuleType();
            description = info.getDescription();
            if (description == null) {
                description = "";
            }
            creator = info.getCreatedBy();
            creationDate = info.getCreated();
            lastUpdater = info.getLastUpdatedBy();
            lastUpdateDate = info.getLastUpdated();
 //           enabled = info.isEnabled();
            if (deleteButton != null) {
                deleteButton.setEnabled(canModify);
            }
            if (replaceButton != null) {
                replaceButton.setEnabled(canModify);
            }
            if (exportButton != null) {
                exportButton.setEnabled(true);
            }
        }
        displayInfo(false);
    }

    private void displayInfo(boolean resetting) {
        settingInitialContents = true;
        moduleNameTFW.setText(moduleName);
        descriptionJTA.setText(description);
//        enabledCB.setSelected(enabled);
        if (detailInfo == null) {
        	moduleNameTFW.setEditable(false);
        	descriptionJTA.setEditable(false);
//        	enabledCB.setEnabled(false);
        } else {
        	moduleNameTFW.setEditable(canModify);
        	descriptionJTA.setEditable(canModify);
//        	enabledCB.setEnabled(canModify);
        }
        if (!resetting) {
            moduleTypeTFW.setText(moduleType);
            creatorTFW.setText(creator);
            if (creationDate != null) {
                creationDateTFW.setText(formatDate(creationDate));
            } else {
                creationDateTFW.setText("");
            }
            updaterTFW.setText(lastUpdater);
            if (lastUpdateDate != null) {
                updateDateTFW.setText(formatDate(lastUpdateDate));
            } else {
                updateDateTFW.setText("");
            }
        }
        settingInitialContents = false;
    }

    public void changeLastUpdatedInfo(Date newDate, String newUpdater) {
        lastUpdater = newUpdater;
        lastUpdateDate = newDate;
        updaterTFW.setText(lastUpdater);
        if (lastUpdateDate != null) {
            updateDateTFW.setText(formatDate(lastUpdateDate));
        } else {
            updateDateTFW.setText("");
        }
    }
    
    private void applyPressed() {
        String newModuleName = null;
        String newDescription = null;
        Boolean newEnabled = null;
        String currentModuleName = moduleNameTFW.getText();
        if (!currentModuleName.equals(moduleName)) {
            newModuleName = currentModuleName;
        }
        String currentDescription = descriptionJTA.getText();
        if (!currentDescription.equals(description)) {
            newDescription = currentDescription;
        }
//        boolean newEnabledStatus = enabledCB.isSelected();
//        if (newEnabledStatus != enabled) {
//            newEnabled = new Boolean(newEnabledStatus);
//        }
        boolean changed = listener.modifyRequested(newModuleName, newDescription, newEnabled);
        if (changed) {
            applyButton.setEnabled(false);
            resetButton.setEnabled(false);
            if (newModuleName != null) {
                moduleName = newModuleName;
            }
            if (newDescription != null) {
                description = newDescription;
            }
//            if (newEnabled != null) {
//                enabled = newEnabled.booleanValue();
//            }
        }
    }

    private void resetPressed() {
        displayInfo(true);
        applyButton.setEnabled(false);
        resetButton.setEnabled(false);
    }

    private void exportPressed() {
        listener.exportRequested();
    }

    private void deletePressed() {
        String msg = "Delete module \"" + moduleName + "\"?";
        int response = CenteredOptionPane.showConfirmDialog(
        		ConsoleMainFrame.getInstance(), msg, 
        		DialogUtility.CONFIRM_DELETE_HDR, JOptionPane.YES_NO_OPTION);
        if (response == JOptionPane.YES_OPTION) {
            listener.deleteRequested();
        }
    }

    private void replacePressed() {
        listener.replaceRequested();
    }

    private String formatDate(Date date) {
        String formattedDate = date.toString();
        return formattedDate;
    }

    private void checkForChanges() {
        if (canModify) {
            boolean changed = false;
            if (!settingInitialContents) {
                changed = (!moduleNameTFW.getText().equals(moduleName));
                if (!changed) {
                    changed = (!descriptionJTA.getText().equals(description));
                }
//                if (!changed) {
//                    changed = (enabledCB.isSelected() != enabled);
//                }
            }
            resetButton.setEnabled(changed);
            applyButton.setEnabled(changed);
        }
    }

    public boolean havePendingChanges() {
        boolean havePending = false;
        if (applyButton != null) {
            havePending = applyButton.isEnabled();
        }
        return havePending;
    }

    public void saveChanges() {
        applyPressed();
    }
}

