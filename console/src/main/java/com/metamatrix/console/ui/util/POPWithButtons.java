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

package com.metamatrix.console.ui.util;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.AbstractButton;
import javax.swing.JPanel;

import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

public class POPWithButtons extends JPanel
        implements PropertyChangeListener {
    private ExpertPropertiedObjectPanelHolder thePanel;
    private PropertiedObjectEditor theEditor;
    private AbstractButton applyButton;
    private AbstractButton resetButton;
    private Map /*<PropertyDefinition> to <InitialAndCurrentValues>*/
            valuesMap = new HashMap();
    private Map /*<String> name to <PropertyDefinition>*/
            defsMap = new HashMap();
    private POPWithButtonsController controller;

    public POPWithButtons(ExpertPropertiedObjectPanelHolder pnl,
            PropertiedObjectEditor edtr,
            POPWithButtonsController ctrlr) {
        super();
        controller = ctrlr;
        theEditor = edtr;
        thePanel = pnl;
        thePanel.getThePanel().addPropertyChangeListener(this);
        init();
        setInitialValues();
        setButtons();
    }

    public POPWithButtons(PropertiedObjectPanel pnl,
            PropertiedObjectEditor editor, POPWithButtonsController controller) {
        this(new ExpertPropertiedObjectPanelHolder(pnl, null), editor, controller);
    }

    private void init() {
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        JPanel buttonsPanel = new JPanel();
        GridBagLayout bl = new GridBagLayout();
        buttonsPanel.setLayout(bl);
        JPanel buttonsInnerPanel = new JPanel(new GridLayout(1, 2, 8, 0));
        applyButton = new ButtonWidget("Apply"); //$NON-NLS-1$
        applyButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                applyPressed();
            }
        });
        resetButton = new ButtonWidget("Reset"); //$NON-NLS-1$
        resetButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                resetPressed();
            }
        });
        buttonsInnerPanel.add(applyButton);
        buttonsInnerPanel.add(resetButton);
        buttonsPanel.add(buttonsInnerPanel);
        bl.setConstraints(buttonsInnerPanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        add(thePanel);
        add(buttonsPanel);
        layout.setConstraints(thePanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(2, 2, 2, 2), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 0), 0, 0));
    }

	public PropertiedObjectEditor getEditor() {
	    return theEditor;
	}
	
    public void setButtons() {
        boolean changed = anyValueChanged();
        applyButton.setEnabled(changed && (!anyValueInvalid()));
        resetButton.setEnabled(changed);
    }

    private void setInitialValues() {
        PropertiedObject propObj = thePanel.getThePanel().getPropertiedObject();
        if (propObj != null) {
        	java.util.List defs = theEditor.getPropertyDefinitions(propObj);
        	Iterator it = defs.iterator();
        	while (it.hasNext()) {
            	PropertyDefinition def = (PropertyDefinition)it.next();
            	defsMap.put(def.getName(), def);
            	Object value = theEditor.getValue(propObj, def);
            	InitialAndCurrentValues vals = new InitialAndCurrentValues(value, 
            			value);
            	valuesMap.put(def, vals);
        	}
        }
        //This had better set the Reset button to disabled, or we have a 
        //problem.
        setButtons();
    }

	public PropertiedObjectPanel getPropertiedObjectPanel() {
	    return thePanel.getThePanel();
	}
	
	public void setPropertiedObject(PropertiedObject propObj) {
		thePanel.getThePanel().setNameColumnHeaderWidth(0);
	    thePanel.getThePanel().setPropertiedObject(propObj);
	    setInitialValues();
	}
	
    private boolean anyValueInvalid() {
        java.util.List /*<PropertyDefinition>*/ invalidDefs =
        		thePanel.getThePanel().getInvalidDefinitions();
		boolean anyInvalid = (invalidDefs.size() > 0);
		return anyInvalid;
    }

    public boolean anyValueChanged() {
        boolean changeFound = false;
        Iterator it = valuesMap.entrySet().iterator();
        while (it.hasNext() && (!changeFound)) {
            Map.Entry me = (Map.Entry)it.next();
            InitialAndCurrentValues vals = (InitialAndCurrentValues)me.getValue();
            if (!vals.valuesEqual()) {
                changeFound = true;
            }
        }
        return changeFound;
    }

    public void propertyChange(PropertyChangeEvent evt) {
        String defName = evt.getPropertyName();
        Object oldValue = evt.getOldValue();
        Object newValue = evt.getNewValue();
        PropertyDefinition defn = (PropertyDefinition)defsMap.get(defName);
        InitialAndCurrentValues vals = (InitialAndCurrentValues)valuesMap.get(defn);
        if (vals == null) {
            vals = new InitialAndCurrentValues(oldValue, newValue);
            valuesMap.put(defn, vals);
        } else {
            vals.setCurrentValue(newValue);
        }
        setButtons();
    }

    public void applyPressed() {
        boolean proceeding = false;
        try {
            StaticUtilities.startWait( ViewManager.getMainFrame() );
            proceeding = controller.doApplyChanges(thePanel.getThePanel());
        } catch (Exception e) {
            ExceptionUtility.showMessage( "Failed modifying CB properties", e ); //$NON-NLS-1$
        } finally {
            StaticUtilities.endWait( ViewManager.getMainFrame() );
        }
		if (proceeding) {
			// now change the 'initial' values to represent the recently
            //  saved values (this is needed to support correct 'reset'
            //  behavior)
            setInitialValues();
            
            //This had better set the Apply and Reset buttons to disabled, or
            //we have a problem.
            setButtons();
        }
    }

    public void resetPressed() {
    //
        try {
            StaticUtilities.startWait( ViewManager.getMainFrame() );
            PropertiedObject propObj = thePanel.getThePanel().getPropertiedObject();
            
			Iterator it = valuesMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry)it.next();
                InitialAndCurrentValues vals = (InitialAndCurrentValues)me.getValue();
                if (!vals.valuesEqual()) {
                    PropertyDefinition def = (PropertyDefinition)me.getKey();
                    theEditor.setValue(propObj, def, vals.getInitialValue());
                    vals.setCurrentValue(vals.getInitialValue());
                }
            }
            thePanel.getThePanel().refreshDisplay();
//            if (scrollBar != null) {
//            	scrollBar.setValue(scrollValue);
//            	this.validate();
//            }

			//This had better set the Apply and Reset buttons to disabled, or
			//we have a problem.
			setButtons();
		} catch ( Exception e ) {
            ExceptionUtility.showMessage( "Failed modifying CB properties", e ); //$NON-NLS-1$
        } finally {
            StaticUtilities.endWait( ViewManager.getMainFrame() );
        }

    }

	public void setButtonsVisible(boolean flag) {
	    applyButton.setVisible(flag);
	    resetButton.setVisible(flag);
	}
	
	public boolean havePendingChanges() {
	    return (applyButton.isVisible() && applyButton.isEnabled());
	}
}//end POPWithButtons
