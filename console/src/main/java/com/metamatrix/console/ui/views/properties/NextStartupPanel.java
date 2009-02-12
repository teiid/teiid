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

package com.metamatrix.console.ui.views.properties;


import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import javax.swing.Icon;
import javax.swing.JPanel;
import javax.swing.JSplitPane;

import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.util.crypto.Encryptor;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;
import com.metamatrix.toolbox.ui.widget.property.PropertySelectionEvent;
import com.metamatrix.toolbox.ui.widget.property.PropertySelectionListener;

public class NextStartupPanel extends DefaultComponentSelector
        implements ActionListener, NotifyOnExitConsole, PropertyChangeListener {
    private String title;
    private Icon icon;
    private JPanel pnlOuter;
    private PropertiedObjectPanel propObjPanel;
    private ConsolePropertiedEditor propEditor;
    private ConsolePropertyObjectId objectedId;
    private PropertyDetailPanel detailPanel;
    private ButtonWidget applyJB;
    private ButtonWidget resetJB;
    private JSplitPane propSplitPane;
    private PropertyFilterPanel parent;
    private boolean buttonState = false;
    private HashMap changeHM= null;
    private HashMap propValueMap = new HashMap();
    private ArrayList changedValue = new ArrayList();
    private ConnectionInfo connection;

    public NextStartupPanel(String title, Icon icon, PropertyFilterPanel parent,
    		ConnectionInfo conn) {
        super();
        this.title = title;
        this.icon = icon;
        this.parent = parent;
        this.connection = conn;
    }

    public void createComponent() {
        PropertiedObjectPanel objectPanel;
        JPanel pnlUp;
        setLayout(new BorderLayout());
        pnlOuter = new JPanel(new BorderLayout());
        applyJB = new ButtonWidget("  Apply  ");
        applyJB.setName("SystemProperties." + title);
        applyJB.setEnabled(buttonState);
        resetJB = new ButtonWidget("  Reset  ");
        resetJB.setName("SystemProperties." + title);
        resetJB.setEnabled(buttonState);
        pnlUp = new JPanel(new BorderLayout());
        JPanel buttonPanel = new JPanel();
        JPanel bpanel = new JPanel(new GridLayout(1, 2, 20, 0));
        bpanel.add(applyJB);
        bpanel.add(resetJB);
        buttonPanel.add(bpanel);
        detailPanel = new PropertyDetailPanel(title);
        if (getObjectPanel() != null) {
            objectPanel  = getObjectPanel();
            objectPanel.createComponent();
            pnlUp.add(objectPanel, BorderLayout.CENTER);
            if (getTitle().equals(PropertiesMasterPanel.NEXT_STARTUP)) {
                pnlUp.add(buttonPanel,BorderLayout.SOUTH);
            }
            propSplitPane = new Splitter(JSplitPane.VERTICAL_SPLIT, true,
                    pnlUp, detailPanel);
            pnlOuter.add(propSplitPane,BorderLayout.CENTER);
            add(pnlOuter, BorderLayout.CENTER);
            applyJB.addActionListener(this);
            resetJB.addActionListener(this);
        }
    }

    public ConsolePropertiedEditor getPropertiedEditor() {
        return propEditor;
    }

    public void setPropertiedEditor(ConsolePropertiedEditor pEditor) {
        this.propEditor = pEditor;
        
        Encryptor encryptor = ModelManager.getConfigurationManager(connection).getEncryptor();
        propObjPanel = new PropertiedObjectPanel(propEditor, encryptor);
        propObjPanel.addPropertyChangeListener(this);
        propObjPanel.setShowExpertProperties(true);
        propObjPanel.setShowHiddenProperties(true);

        if (getTitle().equals(PropertiesMasterPanel.STARTUP)) {
            propObjPanel.setReadOnlyForced(true);
        }
        if (!parent.isModifyServerProperties()) {
            propObjPanel.setReadOnlyForced(true);
        }
        createComponent();
    }

    public void propertyChange(PropertyChangeEvent theEvent) {
         boolean propsDifferent = false;
        // if the property value has been changed before check
        // to see if it now agrees with the original value. if it
        // does check to see if other differences exist
        String eventProp = theEvent.getPropertyName();
        if (propValueMap.containsKey(eventProp)) {
            Object original = propValueMap.get(eventProp);
            Object current = theEvent.getNewValue();
            propsDifferent = !equivalent(original, current);
            if (!propsDifferent) {
                changedValue.remove(eventProp);
            } else {
            	if (!changedValue.contains(eventProp)) {
            		changedValue.add(eventProp);
            	}
            }
        } else {
            // save original value if not previously saved
            // propValueMap contains properties that have changed at one time
            // they may now hold the original value however
            Object original = theEvent.getOldValue();
            Object current = theEvent.getNewValue();
            if (!equivalent(original, current)) {
            	propValueMap.put(eventProp, theEvent.getOldValue());
            	changedValue.add(eventProp);
            }
        }
        if (changedValue.size() > 0) {
            applyJB.setEnabled(true);
            resetJB.setEnabled(true);
        }
        else {
            applyJB.setEnabled(false);
            resetJB.setEnabled(false);
        }
    }

    private boolean equivalent(Object theValue, Object theOtherValue) {
    	boolean same;
    	if (theValue == null) {
    		if (theOtherValue == null) {
    			same = true;
    		} else if ((theOtherValue instanceof String) && 
    				(((String)theOtherValue).length() == 0)) {
    			same = true;
    		} else {
    			same = false;
    		}
    	} else if (theOtherValue == null) {
    		if ((theValue instanceof String) && (((String)theValue).length() ==
    				0)) {
    			same = true;
    		} else {
    			same = false;
    		}
    	} else {
    		same = theValue.equals(theOtherValue);
    	}
    	return same;
    }

    public void postRealize() {
        propSplitPane.setDividerLocation(0.80);
    }

    private PropertiedObjectPanel getObjectPanel() {
        return propObjPanel;
    }

    public String getTitle() {
        return title;
    }

    public Icon getIcon() {
        return icon;
    }
    
    ButtonWidget getApplyButton() {
        return applyJB;
    }

    ButtonWidget getResetButton() {
        return resetJB;
    }

    public void setGroupName(String gName, Object propDef, PropertyFilter pFilter) {
        objectedId = propEditor.getPropertyObjectId(gName, pFilter);
        propEditor.setGroupDefn(gName, (java.util.List)propDef);
        propObjPanel.setNameColumnHeaderWidth(0);
        propObjPanel.setPropertiedObject(objectedId);
        propObjPanel.setColumnHeaderNames("Property Name","Property Value");
        processDetailPanel();
        propObjPanel.addPropertySelectionListener(new PropertySelectionListener() {
            public void propertySelected(PropertySelectionEvent event) {
                processDetailPanel(event.getPropertyDefinition());
            }

        });
        propObjPanel.setNameColumnHeaderWidth(0);
        propObjPanel.refreshDisplay();
        validate();
        repaint();
    }

    private void doChange() {
        if (getPropertiedEditor().getChangeHM() != null) {
            changeHM = getPropertiedEditor().getChangeHM();
        }
        propEditor.setChangePropValue(null,changeHM); // TODO: Is it right if property value set null;
    }

    public void actionPerformed(ActionEvent e) {
        if (e.getActionCommand().equals("  Apply  ")) {
            try {
                StaticUtilities.startWait(this);
                doChange();
                propValueMap.clear();
                changedValue.clear();
            } catch (Exception exc ) {
            } finally {
                StaticUtilities.endWait(this);
            }
            applyJB.setEnabled(false);
            resetJB.setEnabled(false);
        } else if  (e.getActionCommand().equals("  Reset  ")) {
            try {
                StaticUtilities.startWait(this);
                propEditor.resetPropValue();
                applyJB.setEnabled(false);
                resetJB.setEnabled(false);
                parent.refresh(title);
            } catch (Exception exc ) {
            } finally {
                StaticUtilities.endWait(this);
            }

        }
        buttonState = false;
        getPropertiedEditor().getChangeHM().clear();
    }

    private void processDetailPanel() {
        detailPanel.setDisplayName(" ");
        detailPanel.setNSUPropertyValue(" ");
        detailPanel.setIdentifierField(" ");
        detailPanel.setDescriptionName(" ");
    }

    private void processDetailPanel(PropertyDefinition propDef) {
        Properties propNSU = propEditor.getNSUProperties();
        String nsuValue;
        String dispPropName="";
        dispPropName = propDef.getName();
        String dn = (String)objectedId.getDescriptionHM().get(dispPropName);
        if (propNSU.get(dispPropName) !=null) {
            nsuValue = propNSU.get(dispPropName).toString();
        } else {
            nsuValue = "";
        }
        detailPanel.setDisplayName(propDef.getDisplayName());
        detailPanel.setIdentifierField(dispPropName);
        detailPanel.setDescriptionName(dn);
        detailPanel.setNSUPropertyValue(nsuValue);
        setSelectedComponent(detailPanel);
    }

    public boolean havePendingChanges() {
        boolean pending = false;
        if (applyJB != null) {
            pending = applyJB.isEnabled();
        }
        return pending;
    }

    public boolean finishUp() {
        boolean stayingHere = false;
        String msg = "Save changes to " + title + " configuration?";
        int response = DialogUtility.showPendingChangesDialog(msg,
        		connection.getURL(), connection.getUser());
        switch (response) {
            case DialogUtility.YES:
                if (applyJB.isEnabled()) {
                    if (getPropertiedEditor().getChangeHM() != null) {
                        changeHM = getPropertiedEditor().getChangeHM();
                    }
                }
                propEditor.setChangePropValue(title,changeHM);
                stayingHere = false;
                applyJB.setEnabled(false);
                resetJB.setEnabled(false);
                propValueMap.clear();
                changedValue.clear();
                break;
            case DialogUtility.NO:
                stayingHere = false;
                applyJB.setEnabled(false);
                resetJB.setEnabled(false);
                propValueMap.clear();
                changedValue.clear();
                break;
            case DialogUtility.CANCEL:
                stayingHere = true;
                break;
        }
        if  (!getPropertiedEditor().getChangeHM().isEmpty()) {
            getPropertiedEditor().getChangeHM().clear();
        }
        return (!stayingHere);
    }
}
