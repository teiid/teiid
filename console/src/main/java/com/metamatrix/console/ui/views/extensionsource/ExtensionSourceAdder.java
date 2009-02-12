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

import java.awt.Color;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import javax.swing.AbstractButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemEntry;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.models.ExtensionSourceManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.MDCPOpenStateListener;
import com.metamatrix.console.ui.util.ModifiedDirectoryChooserPanel;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

public class ExtensionSourceAdder {

    public static String getFileName(File file, int maxLengthAllowed,
            boolean removeExtension) {
        String nameWithExtension = file.getName();
        String nameToUse;
        if (removeExtension) {
            int dotPosit = -1;
            int posit = nameWithExtension.length() - 1;
            while ((posit >= 0) && (dotPosit < 0)) {
                char curChar = nameWithExtension.charAt(posit);
                if (curChar == '.') {
                    dotPosit = posit;
                } else {
                    posit--;
                }
            }
            if (dotPosit < 0) {
                dotPosit = nameWithExtension.length();
            }
            nameToUse = nameWithExtension.substring(0, dotPosit);
        } else {
            nameToUse = nameWithExtension;
        }
        if (nameToUse.length() > maxLengthAllowed) {
            nameToUse = nameToUse.substring(0, maxLengthAllowed);
        }
        return nameToUse;
    }

    public static String getFileExtension(File file) {
        String nameWithExtension = file.getName();
        String extension = "";
        int dotPosit = -1;
        int posit = nameWithExtension.length() - 1;
        while ((posit >= 0) && (dotPosit < 0)) {
            char curChar = nameWithExtension.charAt(posit);
            if (curChar == '.') {
                dotPosit = posit;
            } else {
                posit--;
            }
        }
        if (dotPosit >= 0) {
            extension = nameWithExtension.substring(dotPosit + 1,
                    nameWithExtension.length());
        }
        return extension;
    }

    public static byte[] contentsOf(File file) throws Exception {
        InputStream stream = new FileInputStream(file);
        int length = (int)file.length();
        byte[] contents = new byte[length];
        int pos = 0;
        while (pos < length) {
            int n= stream.read(contents, pos, contents.length - pos);
            pos += n;
        }
        return contents;
    }

    private ExtensionSourceManager manager;
    private String[] fileTypes;
    private JPanel currentPanel;
    private NewFileSelectorPanel selectorPanel;
    private NewFileInfoPanel infoPanel;
    private AddConfirmationPanel confirmationPanel;
//    private NewExtensionSourceInfo moduleInfo = null;
    private WizardPanelDialog dialog;
    private boolean cancelled = false;
    private boolean finished = false;
    private byte[] fileContents = null;

    public ExtensionSourceAdder(ExtensionSourceManager mgr, String[] fTypes) {
        super();
        manager = mgr;
        fileTypes = fTypes;
    }

    public String go() {
        //1.  Display file selector for user to select file.
        //2.  If file does not exist, display dialog and return to 1.
        //3.  If module by name of file exists, display dialog warning that
        //name must be changed.
        //4.  Display panel for entering file name to be saved as, type,
        //description and enabled.
        //5.  If selected name to be saved as exists, display dialog that name
        //must be changed.
        //6.  Display confirmation of entered info.
        AdderWizardPanel wizardPanel = new AdderWizardPanel(this);
        String moduleName = null;
        String initialDirectory = (String)UserPreferences.getInstance().getValue(
                ExtensionSourcesPanel.EXTENSION_MODULES_INITIAL_FOLDER_KEY);
        selectorPanel = new NewFileSelectorPanel(wizardPanel, initialDirectory);
        infoPanel = new NewFileInfoPanel(fileTypes, wizardPanel);
        confirmationPanel = new AddConfirmationPanel(wizardPanel);
        wizardPanel.addPage(selectorPanel);
        wizardPanel.addPage(infoPanel);
        wizardPanel.addPage(confirmationPanel);
        wizardPanel.getNextButton().setEnabled(false);
        currentPanel = selectorPanel;
        wizardPanel.getCancelButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        wizardPanel.getFinishButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                if (currentPanel == confirmationPanel) {
                    finishPressed();
                }
            }
        });
        dialog = new WizardPanelDialog(this, wizardPanel);
        dialog.show();
        if (finished && (!cancelled)) {
            try {
                NewExtensionSourceInfo exInfo = infoPanel.getInfo(fileContents);
                manager.addModule(exInfo);
                moduleName = exInfo.getModuleName();
                String directoryForModule = selectorPanel.getDirectoryName();
                UserPreferences.getInstance().setValue(
                        ExtensionSourcesPanel.EXTENSION_MODULES_INITIAL_FOLDER_KEY,
                        directoryForModule);
                UserPreferences.getInstance().saveChanges();
            } catch (Exception ex) {
                String msg = "Error adding extension module.";
                LogManager.logError(LogContexts.EXTENSION_SOURCES, ex, msg);
                ExceptionUtility.showMessage(msg, ex);
            }
        }
        selectorPanel = null;
        infoPanel = null;
        confirmationPanel = null;
        wizardPanel = null;
        return moduleName;
    }

    public void dialogWindowClosing() {
        cancelled = true;
    }

    private void cancelPressed() {
        dialog.cancelPressed();
        cancelled = true;
    }

    private void finishPressed() {
        dialog.finishPressed();
        finished = true;
    }

    public boolean showNextPage() {
        boolean goingToNextPage = true;
        if (currentPanel == selectorPanel) {
            String fileName = selectorPanel.getSelectedFileName();
            File file = new File(fileName);
            if (!file.exists()) {
                StaticUtilities.displayModalDialogWithOK("Unable to open file",
                        "Unable to open file " + fileName + ".  Must select " +
                        "a different file or cancel.");
                goingToNextPage = false;
            } else {
                try {
                    fileContents = ExtensionSourceAdder.contentsOf(file);
                } catch (Exception ex) {
                    StaticUtilities.displayModalDialogWithOK("Unable to read file",
                            "Unable to read file " + fileName +
                            ".  Must select a different file or cancel.");
                    goingToNextPage = false;
                }
            }
            if (goingToNextPage) {
                String moduleName = ExtensionSourceAdder.getFileName(file,
                        ExtensionSourceAdminAPI.SOURCE_NAME_LENGTH_LIMIT, false);
                String extension = ExtensionSourceAdder.getFileExtension(file);
                infoPanel.setDisplayInfo(fileName, extension, moduleName);
                currentPanel = infoPanel;
            }
        } else if (currentPanel == infoPanel) {
            String saveAsName = infoPanel.getSaveAsName();
            boolean exists = false;
            try {
                exists = manager.moduleExists(saveAsName);
            } catch (Exception ex) {
                LogManager.logError(LogContexts.EXTENSION_SOURCES, ex,
                        "Error determining if module exists.");
                ExceptionUtility.showMessage(
                        "System error determining if an extension module exists",
                        ex);
                goingToNextPage = false;
            }
            if (exists) {
                String msg = "Module \"" + saveAsName + "\" already exists.  " +
                        "Must change the name or cancel.";
                StaticUtilities.displayModalDialogWithOK("Module already exists",
                        msg);
                infoPanel.giveFocusToSaveAs();
                goingToNextPage = false;
            }
            if (goingToNextPage) {
                NewExtensionSourceInfo info = infoPanel.getInfo(fileContents);
                confirmationPanel.setInfo(info);
                currentPanel = confirmationPanel;
            }
        }
        return goingToNextPage;
    }

    public void showPreviousPage() {
        if (currentPanel == infoPanel) {
            currentPanel = selectorPanel;
        } else if (currentPanel == confirmationPanel) {
            currentPanel = infoPanel;
        }
    }
}//end ExtensionSourceAdder




class AdderWizardPanel extends WizardInterfaceImpl {
    private ExtensionSourceAdder controller;

    public AdderWizardPanel(ExtensionSourceAdder cntrlr) {
        super();
        controller = cntrlr;
    }

    public void showNextPage() {
        boolean continuing = controller.showNextPage();
        if (continuing) {
            super.showNextPage();
        }
    }

    public void showPreviousPage() {
        controller.showPreviousPage();
        super.showPreviousPage();
    }
}//end AdderWizardPanel




class NewFileSelectorPanel extends BasicWizardSubpanelContainer 
		implements MDCPOpenStateListener {
    private ModifiedDirectoryChooserPanel chooser;

    public NewFileSelectorPanel(WizardInterface wizardInterface,
            String initialDirectory) {
        super(wizardInterface);
        FileSystemView view = new FileSystemView();
        if ((initialDirectory != null) && (initialDirectory.length() > 0)) {
            try {
                DirectoryEntry dirEntry = view.lookup(initialDirectory);
                view.setHome(dirEntry);
            } catch (Exception ex) {
                //Any exception that may occur on setting the initial view is
                //inconsequential.  This is merely a convenience to the user.
            }
        }
        chooser = new ModifiedDirectoryChooserPanel(view, 
        		DirectoryChooserPanel.TYPE_OPEN, this);
        chooser.setShowAcceptButton(false);
        chooser.setShowCancelButton(false);
        chooser.setShowDetailsButton(false);
        chooser.setShowFilterComboBox(false);
        chooser.setShowNewFolderButton(false);
        chooser.setShowPassThruFilter(false);
        super.setStepText(1, "Select file containing new extension module");
        super.setMainContent(chooser);
    }

	public void fileSelectionIsValid(boolean flag) {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
		forwardButton.setEnabled(flag);
    }

    public String getSelectedFileName() {
        String name = null;
        FileSystemEntry fse = (FileSystemEntry)chooser.getSelectedTreeNode();
        if (fse != null) {
            name = fse.getFullName();
        }
        return name;
    }

    public String getDirectoryName() {
        String directoryName = null;
        String fullName = getSelectedFileName();
        if (fullName != null) {
            int index = fullName.lastIndexOf(File.separatorChar);
            directoryName = fullName.substring(0, index);
        }
        return directoryName;
    }
}//end NewFileSelectorPanel




class NewFileInfoPanel extends BasicWizardSubpanelContainer {
	private String initialModuleName;
    private String[] fileTypes;
    private JPanel panel = null;
    private TextFieldWidget fileNameTFW;
    private TextFieldWidget saveAsTFW;
    private JComboBox fileTypesComboBox;
//    private JCheckBox enabledCheckBox;
    private JTextArea descriptionTextArea;

    public NewFileInfoPanel(String[] fTypes, WizardInterface wizardInterface) {
        super(wizardInterface);
        fileTypes = new String[fTypes.length + 1];
        fileTypes[0] = "";
        for (int i = 0; i < fTypes.length; i++) {
            fileTypes[i + 1] = fTypes[i];
        }
		panel = init();
        super.setStepText(2, "Enter extension module information.");
        super.setMainContent(panel);
    }

    private JPanel init() {
        JPanel panel = new JPanel();
        GridBagLayout layout = new GridBagLayout();
        panel.setLayout(layout);
        fileNameTFW = new TextFieldWidget();
        fileNameTFW.setEditable(false);
        DefaultTextFieldModel document = new DefaultTextFieldModel();
        document.setMaximumLength(
                ExtensionSourceAdminAPI.SOURCE_NAME_LENGTH_LIMIT);
        saveAsTFW = new TextFieldWidget();
        saveAsTFW.setDocument(document);
        saveAsTFW.getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                changeMade();
            }
            public void insertUpdate(DocumentEvent ev) {
                changeMade();
            }
            public void removeUpdate(DocumentEvent ev) {
                changeMade();
            }
        });
        fileTypesComboBox = new JComboBox(fileTypes);
        fileTypesComboBox.setEditable(false);
        fileTypesComboBox.setBackground(Color.white);
        addComboBoxActionListener();
//        enabledCheckBox = new CheckBox();
//        enabledCheckBox.setSelected(true);
        DefaultTextFieldModel document2 = new DefaultTextFieldModel();
        document2.setMaximumLength(
                ExtensionSourceAdminAPI.SOURCE_DESCRIPTION_LENGTH_LIMIT);
        descriptionTextArea = new JTextArea(document2);
        descriptionTextArea.setRows(3);
        descriptionTextArea.setText("");
        descriptionTextArea.setLineWrap(true);
        descriptionTextArea.setWrapStyleWord(true);
        descriptionTextArea.setMinimumSize(descriptionTextArea.getPreferredSize());
        LabelWidget moduleFileLabel = new LabelWidget("File containing module:");
        LabelWidget saveAsLabel = new LabelWidget("*Save as module name:");
        setBoldFont(saveAsLabel);
        LabelWidget typeLabel = new LabelWidget("*Module type:");
        setBoldFont(typeLabel);
//        LabelWidget enabledLabel = new LabelWidget("Enabled:");
        LabelWidget descriptionLabel = new LabelWidget("Description:");
        LabelWidget requiredFieldLabel = new LabelWidget("*Required field");
        panel.add(fileNameTFW);
        panel.add(saveAsTFW);
        panel.add(fileTypesComboBox);
//        panel.add(enabledCheckBox);
        panel.add(descriptionTextArea);
        panel.add(moduleFileLabel);
        panel.add(saveAsLabel);
        panel.add(typeLabel);
//        panel.add(enabledLabel);
        panel.add(descriptionLabel);
        panel.add(requiredFieldLabel);
        setBoldFont(requiredFieldLabel);
        layout.setConstraints(moduleFileLabel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(10, 5, 5, 5), 0, 0));
        layout.setConstraints(saveAsLabel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(typeLabel, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
//        layout.setConstraints(enabledLabel, new GridBagConstraints(0, 3, 1, 1,
//                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
//                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(descriptionLabel, new GridBagConstraints(0, 4, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 10, 5), 0, 0));
        layout.setConstraints(requiredFieldLabel, new GridBagConstraints(0, 5,
                1, 1, 0.0, 1.0, GridBagConstraints.NORTHEAST, GridBagConstraints.NONE,
                new Insets(15, 5, 5, 5), 0, 0));
        layout.setConstraints(fileNameTFW, new GridBagConstraints(1, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(saveAsTFW, new GridBagConstraints(1, 1, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(fileTypesComboBox, new GridBagConstraints(1, 2, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
//        layout.setConstraints(enabledCheckBox, new GridBagConstraints(1, 3, 1, 1,
//                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
//                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(descriptionTextArea, new GridBagConstraints(1, 4, 1, 1,
                1.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
        return panel;
    }

    private void setBoldFont(LabelWidget label) {
        Font tempFont = label.getFont();
        Font newFont = new Font(tempFont.getName(), Font.BOLD, tempFont.getSize());
        label.setFont(newFont);
    }

    private void addComboBoxActionListener() {
        fileTypesComboBox.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                changeMade();
            }
        });
    }

    public void setDisplayInfo(String fileName, String fileExtension,
            String initModuleName) {
        initialModuleName = initModuleName;
        fileNameTFW.setText(fileName);
        saveAsTFW.setText(initialModuleName);
        selectFileTypeForExtension(fileExtension);
    }

    private void selectFileTypeForExtension(String extension) {
        int selectionLoc = 0;
        int jarLoc = -1;
        int numEntries = fileTypesComboBox.getModel().getSize();
        for (int i = 0; i < numEntries; i++) {
            if (jarLoc >= 0) {
                break;
            }
            String curEntry = fileTypesComboBox.getModel().getElementAt(i)
            		.toString().trim();
            if (curEntry.length() >= 3) {
            	String startOfCurEntry = curEntry.substring(0, 3);
            	if ((jarLoc < 0) && startOfCurEntry.equalsIgnoreCase("jar")) {
                	jarLoc = i;
            	}
            }
        }
        if (extension.equalsIgnoreCase("jar") && (jarLoc >= 0)) {
            selectionLoc = jarLoc;
        }
		fileTypesComboBox.setSelectedIndex(selectionLoc);
    }

    public String getSaveAsName() {
        return saveAsTFW.getText();
    }

    public void giveFocusToSaveAs() {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                saveAsTFW.requestFocus();
            }
        });
    }

    private void changeMade() {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(entriesLegal());
    }

    private boolean entriesLegal() {
        boolean legal = true;
        String moduleType = (String)fileTypesComboBox.getSelectedItem();
        if (moduleType == null) {
        	legal = false;
        } else if (moduleType.equals("")) {
            legal = false;
        } else {
            int docLength = saveAsTFW.getDocument().getLength();
            String saveAs = null;
            try {
                saveAs = saveAsTFW.getDocument().getText(0, docLength);
            } catch (Exception ex) {
                //Cannot occur.
            }
            saveAs = saveAs.trim();
            if (saveAs.equals("")) {
                legal = false;
            }
        }
        return legal;
    }

    public NewExtensionSourceInfo getInfo(byte[] fileContents) {
        String moduleName = saveAsTFW.getText();
        String moduleType = fileTypesComboBox.getSelectedItem().toString();
//        boolean enabled = enabledCheckBox.isSelected();
        String description = descriptionTextArea.getText();
        NewExtensionSourceInfo info = new NewExtensionSourceInfo(
                fileNameTFW.getText(), moduleName, moduleType, description,
                true, fileContents);
        return info;
    }
}//end NewFileInfoPanel




class AddConfirmationPanel extends BasicWizardSubpanelContainer {
    private JPanel panel = null;
    private TextFieldWidget filePathTFW;
    private TextFieldWidget lengthTFW;
    private TextFieldWidget moduleNameTFW;
    private TextFieldWidget moduleTypeTFW;
//    private JCheckBox enabledCheckBox;
    private JTextArea descriptionTextArea;

    public AddConfirmationPanel(WizardInterface wizardInterface) {
        super(wizardInterface);
        super.setStepText(3, 
        		"Confirmation.  Press \"Finish\" to add the module.");
    }

    public void setInfo(NewExtensionSourceInfo info) {
        if (panel == null) {
            panel = new JPanel();
            GridBagLayout layout = new GridBagLayout();
            panel.setLayout(layout);
            LabelWidget moduleFileLabel = new LabelWidget("Load module from file:");
            panel.add(moduleFileLabel);
            filePathTFW = new TextFieldWidget(info.getFileName());
            panel.add(filePathTFW);
            LabelWidget lengthLabel = new LabelWidget("File length:");
            panel.add(lengthLabel);
            lengthTFW = new TextFieldWidget(info.getFileContents().length +
                    " bytes");
            panel.add(lengthTFW);
            LabelWidget saveAsLabel = new LabelWidget("Module name:");
            panel.add(saveAsLabel);
            moduleNameTFW = new TextFieldWidget(info.getModuleName());
            panel.add(moduleNameTFW);
            LabelWidget typeLabel = new LabelWidget("Module type:");
            panel.add(typeLabel);
            moduleTypeTFW = new TextFieldWidget(info.getModuleType());
            panel.add(moduleTypeTFW);
            LabelWidget enabledLabel = new LabelWidget("Enabled:");
            panel.add(enabledLabel);
//            enabledCheckBox = new CheckBox("");
//            enabledCheckBox.setSelected(info.isEnabled());
//            panel.add(enabledCheckBox);
            LabelWidget descriptionLabel = new LabelWidget("Description:");
            panel.add(descriptionLabel);
            descriptionTextArea = new JTextArea(info.getDescription());
            descriptionTextArea.setRows(3);
            descriptionTextArea.setLineWrap(true);
            descriptionTextArea.setWrapStyleWord(true);
            panel.add(descriptionTextArea);

            filePathTFW.setEditable(false);
            lengthTFW.setEditable(false);
            moduleNameTFW.setEditable(false);
            moduleTypeTFW.setEditable(false);
//            enabledCheckBox.setEnabled(false);
            descriptionTextArea.setEditable(false);

            layout.setConstraints(saveAsLabel, new GridBagConstraints(0, 1, 1, 1,
                    0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                    new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(typeLabel, new GridBagConstraints(0, 2, 1, 1,
                    0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                    new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(moduleFileLabel, new GridBagConstraints(
                    0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
                    GridBagConstraints.NONE, new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(lengthLabel, new GridBagConstraints(0, 4, 1, 1,
                    0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                    new Insets(5, 10, 5, 10), 0, 0));
//            layout.setConstraints(enabledLabel, new GridBagConstraints(
//                    0, 5, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
//                    GridBagConstraints.NONE, new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(descriptionLabel, new GridBagConstraints(0, 6,
                    1, 1, 0.0, 0.0, GridBagConstraints.NORTHEAST,
                    GridBagConstraints.NONE, new Insets(5, 10, 5, 10), 0, 0));

            layout.setConstraints(moduleNameTFW, new GridBagConstraints(
                    1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(moduleTypeTFW, new GridBagConstraints(
                    1, 2, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(filePathTFW, new GridBagConstraints(
                    1, 3, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(lengthTFW, new GridBagConstraints(
                    1, 4, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 5, 10), 0, 0));
//            layout.setConstraints(enabledCheckBox, new GridBagConstraints(
//                    1, 5, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
//                    GridBagConstraints.NONE, new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(descriptionTextArea, new GridBagConstraints(
                    1, 6, 1, 1, 1.0, 1.0, GridBagConstraints.NORTH,
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 5, 10), 0, 0));

            super.setMainContent(panel);
        } else {
            filePathTFW.setText(info.getFileName());
            lengthTFW.setText(info.getFileContents().length + " bytes");
            moduleNameTFW.setText(info.getModuleName());
            moduleTypeTFW.setText(info.getModuleType());
//            enabledCheckBox.setSelected(info.isEnabled());
            descriptionTextArea.setText(info.getDescription());
        }
    }
}//end AddConfirmationPanel




class WizardPanelDialog extends JDialog {
    private ExtensionSourceAdder caller;
    private AdderWizardPanel wizardPanel;
    
    public WizardPanelDialog(ExtensionSourceAdder cllr, AdderWizardPanel wizPnl) {
        super(ConsoleMainFrame.getInstance(), "Add Extension Module Wizard");
        caller = cllr;
        wizardPanel = wizPnl;
        this.setModal(true);
        this.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                caller.dialogWindowClosing();
            }
        });
        init();
        this.pack();
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    private void init() {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        this.getContentPane().add(wizardPanel);
        layout.setConstraints(wizardPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
    }

    public void cancelPressed() {
        this.dispose();
    }

    public void finishPressed() {
        this.dispose();
    }
}//end WizardPanelDialog
