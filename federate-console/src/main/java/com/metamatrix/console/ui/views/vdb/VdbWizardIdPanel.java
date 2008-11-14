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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.TypeConstants;
import com.metamatrix.console.util.ExceptionUtility;

import com.metamatrix.metadata.runtime.api.VirtualDatabase;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

public class VdbWizardIdPanel extends BasicWizardSubpanelContainer
        implements TypeConstants {

    // data
    String sVdbName         = "";
    String sDescription     = "";
    private int TEXTAREA_MAXLENGTH      = 255;

    // used when this panel is doing a new version
    VirtualDatabase vdbSourceVdb        = null;

    boolean NEW_VDB                     = true;
    boolean NEW_VDB_VERSION             = false;
    boolean idInitSuccessful            = true;

    private String sAllowedCharacters
        = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

    JPanel pnlOuter = new JPanel();
    DefaultTextFieldModel dtfmTextModel = new DefaultTextFieldModel();
    JTextArea txaDescription = new JTextArea(dtfmTextModel);
    LabelWidget lblDescription = new LabelWidget();
    TextFieldWidget txfName = GuiComponentFactory.createTextField(VDB_NAME);
    LabelWidget lblName = new LabelWidget();
    
    private int panelOrder;
    
    
    public VdbWizardIdPanel(int step, VirtualDatabase vdb, WizardInterface wizardInterface) {
        super(wizardInterface);
        panelOrder = step;

        NEW_VDB_VERSION     = true;
        NEW_VDB             = false;
        setSourceVdb(vdb);
        try  {
            jbInit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        setSourceVdb(vdb);
        this.sVdbName =getSourceVdb().getName();
        txfName.setEditable(false);

        setDescription(getSourceVdb().getDescription());
        
        putDataIntoPanel();        
    }

    public VdbWizardIdPanel(int step, WizardInterface wizardInterface) {
        super(wizardInterface);
        NEW_VDB_VERSION     = false;
        NEW_VDB             = true;
        panelOrder = step;

        try  {
            jbInit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        this.enableForwardButton(false);
               
        txfName.getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
                checkNextButtonEnabling();
            }
            public void insertUpdate(DocumentEvent ev) {
                checkNextButtonEnabling();
            }
            public void removeUpdate(DocumentEvent ev) {
                checkNextButtonEnabling();
            }
        });        
        
        // trigger the initial setting of the forward button.
        this.getWizardInterface().getForwardButton().setEnabled(
                (sVdbName.trim().length() > 0));

        
    }

    private VirtualDatabase getSourceVdb() {
        return vdbSourceVdb;
    }

    private void setSourceVdb(VirtualDatabase vdb) {
        vdbSourceVdb = vdb;
    }

    public String getVdbName() {
        return this.sVdbName;
    }

    public String getDescription() {
        return sDescription;
    }

    private static final String FINDC=".";

    public void setVdbName(String svdbName) {
        // if this is a vdb version, then its set at
        // init time based on the VDB the user selected,
        // cannot be changed like the new VDB
        if (NEW_VDB_VERSION) {
            return;
        }
        if (svdbName.indexOf(FINDC) > 0) {
            int l = svdbName.indexOf(FINDC);
            this.sVdbName = svdbName.substring(0, l);
            
        } else {
            this.sVdbName = svdbName;
        }

    }

    public void setDescription(String sDesc) {
        this.sDescription = sDesc;       
    }

    public void getDataFromPanel() {
        this.sVdbName = getTextValue(); 
        this.sDescription = txaDescription.getText();
    }

    public void putDataIntoPanel() {
        
        txfName.setText(sVdbName);
        txaDescription.setText(sDescription);
        checkNextButtonEnabling();

        
    }

    private void checkNextButtonEnabling() {
//        int docLength = txfName.getDocument().getLength();
        String nameText = getTextValue();
//        try {
//            nameText = txfName.getDocument().getText(0, docLength).trim();
//        } catch (Exception ex) {
//            //cannot occur
//        }
        
        this.getWizardInterface().getForwardButton().setEnabled(
                (nameText.trim().length() > 0));
    }
    private String getTextValue() {
        return txfName.getText();  
    }
//    private String getTextValue() {
//        int docLength = txfName.getDocument().getLength();
//         String nameText = null;
//         try {
//             nameText = txfName.getDocument().getText(0, docLength).trim();
//         } catch (Exception ex) {
//             //cannot occur
//         }
//          return nameText;        
//    }

    public void postRealize() {
        txfName.requestFocus();
    }

    private void jbInit() throws Exception {
        dtfmTextModel.setMaximumLength(TEXTAREA_MAXLENGTH);
        createPanel();
        setMainContent(pnlOuter);
        if (NEW_VDB) {
            setStepText(panelOrder, "Specify a name and a description for this Virtual Database.");
        } else if (NEW_VDB_VERSION) {
            setStepText(panelOrder, true, "Modify the description for this new version of a Virtual Database.",
            		null);
        }
    }

    public boolean getIdInitSuccessful() {
        return idInitSuccessful;
    }

    private void createPanel() {
        GridBagLayout layout = new GridBagLayout();
        pnlOuter.setLayout(layout);
        txaDescription.setText("");
        txaDescription.setColumns(30);
        txaDescription.setRows(4);
        txaDescription.setLineWrap(true);
        txaDescription.setWrapStyleWord(true);
        txaDescription.setPreferredSize(new Dimension(450, 100));
        txaDescription.setMinimumSize(new Dimension(250, 100));

        try {
            txfName.setValidCharacters(sAllowedCharacters);
        } catch (Exception e) {
            ExceptionUtility.showMessage("Failed setting VDB Name field", e);
            idInitSuccessful = false;
        }

        lblDescription.setText("Description:");
        txfName.setText("");
        lblName.setText("*Name:");
        setBoldFont(lblName);

        LabelWidget requiredFieldLabel = new LabelWidget("*Required field");
        setBoldFont(requiredFieldLabel);

        pnlOuter.add(lblName);
        pnlOuter.add(txfName);
        pnlOuter.add(lblDescription);
        pnlOuter.add(txaDescription);
        pnlOuter.add(requiredFieldLabel);
        layout.setConstraints(lblName, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(txfName, new GridBagConstraints(1, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(lblDescription, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(txaDescription, new GridBagConstraints(1, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        layout.setConstraints(requiredFieldLabel, new GridBagConstraints(0, 2, 1, 1,
                0.0, 1.0, GridBagConstraints.NORTHEAST, GridBagConstraints.NONE,
                new Insets(15, 5, 5, 5), 0, 0));
    }

    private void setBoldFont(LabelWidget label) {
        Font tempFont = label.getFont();
        Font newFont = new Font(tempFont.getName(), Font.BOLD, tempFont.getSize());
        label.setFont(newFont);
    }
}
