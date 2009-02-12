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

package com.metamatrix.console.ui.util.wizard;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.EventListenerList;

/**
 * DefaultWizardClientPanel is a default version of
 *  WizardClientPanel.
 *
 *
 *
 */

public class DefaultWizardClientPanel
              extends JPanel
           implements WizardClientPanel

{
    protected boolean bNextIsEnablable    =   false;
    protected EventListenerList listeners = new EventListenerList();



    public DefaultWizardClientPanel()
    {
        super();
        //createComponent();
    }

    public void createComponent()
    {

    }

    public String getTitle()
    {
        return ""; //$NON-NLS-1$
    }

    // =============
    //  Method(s) for interface: WizardClientPanel
    // =============
    public boolean isNextButtonEnablable()
    {
        return bNextIsEnablable;
    }

    private EventListenerList getListeners(){
        return listeners;
    }


    public void addChangeListener( ChangeListener listener )
    {
        getListeners().add(ChangeListener.class, listener);
    }

    public void removeChangeListener(ChangeListener listener)
    {
        getListeners().remove(ChangeListener.class, listener);
    }

    public JPanel getComponent()
    {
        return this;
    }


    // ==========================
    //  Other supporting methods
    // ==========================
    /**
     *  fireChangedEvent
     *      Let the listeners know when the internal state of
     *      this panel changes from 'The Next button should be enabled'
     *      to 'The Next button should be DISABLED'.  On receiving
     *      this event the listeners will call the
     *      isNextStateEnablable() method to determine the new state.
     *
     */
    protected void fireChangedEvent(ChangeEvent e)
    {
        // Guaranteed to return a non-null array
        Object[] listeners = getListeners().getListenerList();

        // Process the listeners last to first, notifying
        // those ChangeListeners that are interested in this event
        for (int i = listeners.length-2; i>=0; i-=2)
        {
            if (listeners[i] == ChangeListener.class)
            {
                ((ChangeListener)listeners[i+1]).stateChanged(e);
            }
        }
    }


    
}
