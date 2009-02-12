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

import java.awt.event.ActionEvent;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.AbstractButton;
import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.JMenuItem;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;

public abstract class AbstractPanelAction extends AbstractAction {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    public static final String MENU_ITEM_NAME = "menu.item.name"; //$NON-NLS-1$

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private int mnemonic = 0;
    protected int type = -1;
    private HashSet comps = new HashSet();

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public AbstractPanelAction(int theType) {
    	super();
        type = theType;
    }

	public AbstractPanelAction(String menuItemName) {
		this(-1);
		this.putValue(MENU_ITEM_NAME, menuItemName);
	}
	
    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected abstract void actionImpl(ActionEvent theEvent)
        throws ExternalException;

    public void actionPerformed(ActionEvent theEvent) {
        try {
            StaticUtilities.startWait(ViewManager.getMainFrame());
            actionImpl(theEvent);
        }
        catch (Exception theException) {
            handleError(theException);
        }
        finally {
            StaticUtilities.endWait(ViewManager.getMainFrame());
        }
    }

    public void addComponent(JComponent theComponent) {
        comps.add(theComponent);
        if (theComponent instanceof AbstractButton) {
            AbstractButton btn = (AbstractButton)theComponent;
            if (getValue(NAME) != null) {
                btn.setText((String)getValue(NAME));
            }
            if (getValue(SMALL_ICON) != null) {
                btn.setIcon((Icon)getValue(SMALL_ICON));
            }
            if (getValue(MENU_ITEM_NAME) != null) {
                if (theComponent instanceof JMenuItem) {
                    btn.setText((String)getValue(MENU_ITEM_NAME));
                }
            }

            btn.setMnemonic(mnemonic);
            if ( !(btn instanceof JMenuItem ) ) {
                btn.addActionListener(this);
            }
        }
        if (getValue(SHORT_DESCRIPTION) != null) {
            theComponent.setToolTipText((String)getValue(SHORT_DESCRIPTION));
        }
        theComponent.setEnabled(isEnabled());
    }

    public Set getComponents() {
        return comps;
    }

    protected void handleError(Exception theException) {
        Object name = getValue(NAME);
        if (name == null) {
            name = getValue(MENU_ITEM_NAME);
        }
        ExceptionUtility.showMessage("Error Performing Action " + name, //$NON-NLS-1$
                                     theException.getMessage(),
                                     theException);
        LogManager.logError(LogContexts.GENERAL,
                            theException,
                            paramString());
    }

    public int getMnemonic() {
        return mnemonic;
    }

    public void putValue(String theKey, Object theValue) {
        super.putValue(theKey, theValue);
        Iterator itr = comps.iterator();
        while (itr.hasNext()) {
            JComponent comp = (JComponent)itr.next();
            if (comp instanceof AbstractButton) {
                if (theKey.equals(NAME)) {
                    ((AbstractButton)comp).setText((String)theValue);
                }
                else if (theKey.equals(SMALL_ICON)) {
                    ((AbstractButton)comp).setIcon((Icon)theValue);
                }
                else if (theKey.equals(MENU_ITEM_NAME)) {
                    if (comp instanceof JMenuItem) {
                        ((AbstractButton)comp).setText((String)theValue);
                    }
                }
            }
            if (theKey.equals(SHORT_DESCRIPTION)) {
                comp.setToolTipText((String)theValue);
            }
        }
    }

    public void removeComponent(JComponent theComponent) {
        if (comps.remove(theComponent)) {
            if (theComponent instanceof AbstractButton) {
                ((AbstractButton)theComponent).removeActionListener(this);
            }
        }
    }

    public void setEnabled(boolean theEnabledFlag) {
        if (isEnabled() != theEnabledFlag) {
            super.setEnabled(theEnabledFlag);
            Iterator itr = comps.iterator();
            while (itr.hasNext()) {
                JComponent comp = (JComponent)itr.next();
                comp.setEnabled(theEnabledFlag);
            }
        }
    }

    public void setMnemonic(int theMnemonic) {
        mnemonic = theMnemonic;
        Iterator itr = comps.iterator();
        while (itr.hasNext()) {
            JComponent comp = (JComponent)itr.next();
            if (comp instanceof AbstractButton) {
                ((AbstractButton)comp).setMnemonic(theMnemonic);
            }
        }
    }

    public String paramString() {
        return
            new StringBuffer()
                   .append("class=").append(getClass()) //$NON-NLS-1$
                   .append(", type=").append(type) //$NON-NLS-1$
                   .append(", enabled=").append(isEnabled()) //$NON-NLS-1$
                   .append(", attached components=").append(comps.size()) //$NON-NLS-1$
                   .append(", mnemonic=").append(mnemonic) //$NON-NLS-1$
                   .append(", name=").append(getValue(NAME)) //$NON-NLS-1$
                   .append(", menu item name=").append(getValue(MENU_ITEM_NAME)) //$NON-NLS-1$
                   .append(", name=").append(getValue(SHORT_DESCRIPTION)) //$NON-NLS-1$
                   .toString();
    }
    
    public String toString() {
    	String str = "AbstractPanelAction: " + paramString(); //$NON-NLS-1$
    	return str;
    }
}
