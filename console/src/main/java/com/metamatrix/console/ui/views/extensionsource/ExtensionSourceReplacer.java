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
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import javax.swing.AbstractButton;
import javax.swing.JDialog;
import javax.swing.JPanel;

import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.log.LogManager;
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
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class ExtensionSourceReplacer {
    private String moduleName;
    private ExtensionSourceManager manager;
    private JPanel currentPanel;
    private ReplacementFileSelectorPanel selectorPanel;
    private ConfirmationPanel confirmationPanel;
    private byte[] contents;
//    private boolean cancelled = false;
    private boolean finished = false;
    private ReplacerWizardPanelDialog dialog = null;

    public ExtensionSourceReplacer(String module, ExtensionSourceManager mgr) {
        super();
        moduleName = module;
        manager = mgr;
    }

    public boolean go() {
        //1.  Display file selector for user to select replacement file.
        //2.  If file does not exist, display dialog and return to 1.
        //3.  Display confirmation.
        //4.  If unable to open file, display dialog and return to 1.
        ReplacementWizardPanel wizardPanel = new ReplacementWizardPanel(this);
        boolean replaced = false;
        String initialDirectory = (String)UserPreferences.getInstance().getValue(
                ExtensionSourcesPanel.EXTENSION_MODULES_INITIAL_FOLDER_KEY);
        selectorPanel = new ReplacementFileSelectorPanel(moduleName, wizardPanel,
                initialDirectory);
        confirmationPanel = new ConfirmationPanel(wizardPanel);
        wizardPanel.addPage(selectorPanel);
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
        dialog = new ReplacerWizardPanelDialog(this, wizardPanel);
        dialog.show();
        if (finished) {
            try {
                manager.replaceModule(moduleName, contents);
                replaced = true;
                
                String directoryForModule = selectorPanel.getDirectoryName();
                UserPreferences.getInstance().setValue(
                        ExtensionSourcesPanel.EXTENSION_MODULES_INITIAL_FOLDER_KEY,
                        directoryForModule);
                                
                StaticUtilities.displayModalDialogWithOK(
                        "Replacement successful",
                        "Extension module " + moduleName +
                        " successfully replaced with contents of file " +
                        selectorPanel.getSelectedFileName() + ".");
            } catch (ExtensionModuleNotFoundException ex) {
                ExceptionUtility.showMessage("Extension module \"" + moduleName +
                        "\" not found.", ex);
            } catch (Exception ex) {
                ExceptionUtility.showMessage("Replace extension module", ex);
                LogManager.logError(LogContexts.EXTENSION_SOURCES, ex,
                        "Error replacing extension module.");
            }
        }
        selectorPanel = null;
        confirmationPanel = null;
        wizardPanel = null;
        return replaced;
    }

    public void dialogWindowClosing() {
//        cancelled = true;
    }

    private void cancelPressed() {
        dialog.cancelPressed();
//        cancelled = true;
    }

    private void finishPressed() {
        dialog.finishPressed();
        finished = true;
    }

    public boolean showNextPage() {
        boolean continuing = true;
        if (currentPanel == selectorPanel) {
            try {
                contents = selectorPanel.getSelectedFileContents();
            } catch (Exception ex) {
                StaticUtilities.displayModalDialogWithOK("Unable to open file",
                        "Unable to open file " +
                        selectorPanel.getSelectedFileName() + ".  Must select " +
                        "a different file or cancel.");
                continuing = false;
            }
            if (continuing) {
                confirmationPanel.init(moduleName,
                        selectorPanel.getSelectedFileName(), contents.length);
                currentPanel = confirmationPanel;
            }
        }
        return continuing;
    }

    public void showPreviousPage() {
        currentPanel = selectorPanel;
    }
}//end ExtensionSourceReplacer




class ReplacementWizardPanel extends WizardInterfaceImpl {
    private ExtensionSourceReplacer controller;

    public ReplacementWizardPanel(ExtensionSourceReplacer cntrlr) {
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
}




class ReplacementFileSelectorPanel extends BasicWizardSubpanelContainer 
		implements MDCPOpenStateListener {
    private String moduleName;
    private ModifiedDirectoryChooserPanel chooser;

    public ReplacementFileSelectorPanel(String module,
            WizardInterface wizardInterface, String initialDirectory) {
        super(wizardInterface);
        moduleName = module;
        FileSystemView view = new FileSystemView();
        if ((initialDirectory != null) && (initialDirectory.length() > 0)) {
            try {
                view.setHome(view.lookup(initialDirectory));
            } catch (Exception ex) {
                //Any exception that may occur in setting the initial view is
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
        super.setStepText(1, "Select file containing new contents for module " 
        		+ moduleName);
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
            name = fse.getName();
            name = fse.getFullName();
        } else {
            String directory = chooser.getParentDirectoryEntry().toString();
            if (!directory.endsWith(File.separator)) {
                directory = directory + File.separator;
            }
            String fileNameEntered = chooser.getNameFieldText().trim();
            name = directory + fileNameEntered;
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

    public byte[] getSelectedFileContents() throws Exception {
        File file = new File(getSelectedFileName());
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
}//end ReplacementFileSelectorPanel




class ConfirmationPanel extends BasicWizardSubpanelContainer {
    private JPanel panel = null;
    private LabelWidget mainLabel;
    private TextFieldWidget fileText;
    private TextFieldWidget lengthText;

    public ConfirmationPanel(WizardInterface wizardInterface) {
        super(wizardInterface);
        super.setStepText(2, "Confirmation.  Press \"Finish\" to proceed with change.");
    }

    public void init(String moduleName, String fileName, int length) {
        if (panel == null) {
            panel = new JPanel();
            GridBagLayout layout = new GridBagLayout();
            panel.setLayout(layout);
            mainLabel = new LabelWidget("Replace module \"" +
                    moduleName + "\" with contents of:");
            panel.add(mainLabel);
            LabelWidget fileLabel = new LabelWidget("File:");
            panel.add(fileLabel);
            fileText = new TextFieldWidget(fileName);
            fileText.setEditable(false);
            panel.add(fileText);
            LabelWidget lengthLabel = new LabelWidget("Length:");
            panel.add(lengthLabel);
            lengthText = new TextFieldWidget(length + " bytes");
            lengthText.setEditable(false);
            panel.add(lengthText);
            layout.setConstraints(mainLabel, new GridBagConstraints(0, 0, 2, 1,
                    0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                    new Insets(10, 10, 10, 10), 0, 0));
            layout.setConstraints(fileLabel, new GridBagConstraints(0, 1, 1, 1,
                    0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                    new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(fileText, new GridBagConstraints(1, 1, 1, 1,
                    1.0, 0.0, GridBagConstraints.CENTER, 
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 5, 10), 
                    0, 0));
            layout.setConstraints(lengthLabel, new GridBagConstraints(0, 2, 1, 1,
                    0.0, 0.0, GridBagConstraints.NORTHEAST, 
                    GridBagConstraints.NONE, new Insets(5, 10, 5, 10), 0, 0));
            layout.setConstraints(lengthText, new GridBagConstraints(1, 2, 1, 1,
                    1.0, 1.0, GridBagConstraints.NORTHWEST, 
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 5, 10), 
                    0, 0));
            super.setMainContent(panel);
        } else {
            mainLabel.setText("Replace module \"" + moduleName +
                    "\" with contents of:");
            fileText.setText(fileName);
            lengthText.setText(length + " bytes");
        }
    }
}//end ConfirmationPanel




class ReplacerWizardPanelDialog extends JDialog {
    private ExtensionSourceReplacer caller;
    private ReplacementWizardPanel wizardPanel;

    public ReplacerWizardPanelDialog(ExtensionSourceReplacer cllr,
            ReplacementWizardPanel wizPnl) {
        super(ConsoleMainFrame.getInstance(), "Replace Extension Module Wizard");
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
}//end ReplacerWizardPanelDialog
