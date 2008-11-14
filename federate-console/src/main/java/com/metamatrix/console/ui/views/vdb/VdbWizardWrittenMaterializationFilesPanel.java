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
import java.awt.event.ItemListener;
import java.awt.event.ItemEvent;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;

public class VdbWizardWrittenMaterializationFilesPanel extends BasicWizardSubpanelContainer {
    
    private JPanel thePanel;
    private boolean overwrite = false;
    protected boolean alreadyWritten;
    
    public VdbWizardWrittenMaterializationFilesPanel(WizardInterface wizardInterface, 
            int stepNum,
            boolean alreadyWritten) {
        super(wizardInterface);
        
        this.alreadyWritten = alreadyWritten;
        String title;
        if (alreadyWritten) {
            title = "Materialization Files Written"; //$NON-NLS-1$
        } else {
            title = "Materialization Files to be Written"; //$NON-NLS-1$
        }

        super.setStepText(stepNum, title);
        thePanel = createPanel();
        super.setMainContent(thePanel);
    }

    private JPanel createPanel() {
        JPanel panel = new JPanel();
        
        return panel;
    }

    public void setResults(Object info) {
        thePanel.removeAll();
        
        GridLayout layout = new GridLayout(2, 1);
        thePanel.setLayout(layout);
        JPanel contents = new JPanel();
        if (info instanceof AllMaterializationFilesDisplayInfo) {
            contents = new MaterializationInfoPanel((AllMaterializationFilesDisplayInfo)info, alreadyWritten);
        } else if (info instanceof Throwable) {
            contents = new ViewErrorPanel((Throwable)info);
        }
        thePanel.add(contents);
        
        
        if (! alreadyWritten) {
            CheckBox overwriteCheckBox = new CheckBox("Overwrite files if they exist?");  //$NON-NLS-1$
            overwriteCheckBox.addItemListener(new ItemListener() {
               public void itemStateChanged(ItemEvent e) {
                   overwrite = (e.getStateChange() == ItemEvent.SELECTED);
               }
            });
            JPanel overwriteCheckBoxPanel = new JPanel();
            overwriteCheckBoxPanel.add(overwriteCheckBox);
            thePanel.add(overwriteCheckBoxPanel);
        }
        
        
    }
    
    
    public boolean getOverwrite() {
        return overwrite;
    }
}//end VdbWizardWrittenMaterializationFilesPanel




class MaterializationInfoPanel extends JPanel {
    
    private final static int HORIZONTAL_INSETS = 10;
    
    private JLabel dir;
    private JPanel filesPanel;
    
    public MaterializationInfoPanel(AllMaterializationFilesDisplayInfo info, boolean alreadyWritten) {
        super();
        init(info, alreadyWritten);
    }
    
    private void init(AllMaterializationFilesDisplayInfo info, boolean alreadyWritten) {
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        
        String header;
        if (alreadyWritten) {
            header = "The following materialization files were written to folder "; //$NON-NLS-1$
        } else {
            header = "The following materialization files will be written to folder "; //$NON-NLS-1$
        }
            
        JLabel hdr = new LabelWidget(header);
        add(hdr);
        layout.setConstraints(hdr, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, 
                new Insets(10, HORIZONTAL_INSETS, 5, 0),
                0, 0));
        dir = new LabelWidget();
        add(dir);
        layout.setConstraints(dir, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, HORIZONTAL_INSETS + 20, 0, 0), 0, 0));
        filesPanel = new JPanel();
        add(filesPanel);
        layout.setConstraints(filesPanel, new GridBagConstraints(0, 2, 1, 1, 0.0, 1.0,
                GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL, 
                new Insets(10, HORIZONTAL_INSETS + 5, 10, HORIZONTAL_INSETS + 5),
                0, 0));
        filesPanel.setBorder(BorderFactory.createEtchedBorder());
        setInfo(info);
    }
    
    public void setInfo(AllMaterializationFilesDisplayInfo info) {
        dir.setText(info.getFolderName() + ':');
        filesPanel.removeAll();
        GridBagLayout filesLayout = new GridBagLayout();
        filesPanel.setLayout(filesLayout);
        SingleMaterializationFileDisplayInfo[] files = info.getFiles();
        for (int i = 0; i < files.length; i++) {
            String desc = files[i].getDescription();
            String name = files[i].getName();
            if (desc != null) {
                LabelWidget descLbl = new LabelWidget(desc + ':');
                filesPanel.add(descLbl);
                filesLayout.setConstraints(descLbl, new GridBagConstraints(0, i, 1, 1,
                        0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE, 
                        new Insets(2, 10, 2, 10), 0, 0));
            }
            LabelWidget nameLbl = new LabelWidget(name);
            filesPanel.add(nameLbl);
            filesLayout.setConstraints(nameLbl, new GridBagConstraints(1, i, 1, 1,
                    0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                    new Insets(2, 10, 2, 10), 0, 0));
        }
    }
}//end MaterializationInfoPanel




class ViewErrorPanel extends JPanel {
    private Throwable theError;
    
    public ViewErrorPanel(Throwable t) {
        super();
        theError = t;
        init();
    }
    
    private void init() {
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        JLabel label = new LabelWidget(
                "An error occurred in attempting to save the materialization files."); //$NON-NLS-1$
        this.add(label);
        layout.setConstraints(label, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(20, 0, 20, 0), 0, 0));
        JButton button = new ButtonWidget("View Error Dialog"); //$NON-NLS-1$
        this.add(button);
        layout.setConstraints(button, new GridBagConstraints(0, 1, 1, 1, 0.0, 1.0,
                GridBagConstraints.NORTH, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 0), 0, 0));
        button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                viewError();
            }
        });
    }
    
    private void viewError() {
        ExceptionUtility.showMessage("Error in saving materialization file", theError); //$NON-NLS-1$
    }
}//end ViewErrorPanel
