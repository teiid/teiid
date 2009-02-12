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


import java.util.Vector;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.EventListenerList;

public abstract class AbstractWizardClient
              extends Object
           implements WizardClient
{

    private java.util.List lstPanels            = new Vector();
    private int iFirstPanelIndex                = 0;
    private String sTitle                       = "Default Wizard Title"; //$NON-NLS-1$
    private boolean bCancelClicked              = false;
    private boolean bFinishClicked              = false;


    // This class will manage a set of ChangeListeners,
    //   Subclasses will fire events when their 'finishClicked' methods
    //   are called.
    private EventListenerList listeners;


    public AbstractWizardClient( String sTitle )
    {
        this.sTitle = sTitle;
        
    }

    protected void init()
    {

    }

    public java.util.List getPanels()
    {

        if ( lstPanels == null )
        {
            lstPanels   = new Vector();
        }

        return lstPanels;
    }

    public void addPanel( WizardClientPanel wcp )
    {
        getPanels().add( wcp );


    }

    public void setFirstPanelIndex( int iIndex )
    {
        iFirstPanelIndex = iIndex;
    }

    public int getFirstPanelIndex()
    {
        return iFirstPanelIndex;
    }

    public String getTitle()
    {
        return sTitle;
    }


    /*  Do not implement these, so the subclasses will be forced to:
           panelsChanging; cancelClicked; finishClicked
    public void panelsChanging( int iCurrPanel, int iNextPanel )
    {

    }
    */

    public void cancelClicked()
    {
        bCancelClicked = true;
        fireChangedEvent( new ChangeEvent( this ) );
    }

    public boolean isCancelClicked()
    {
        return bCancelClicked;
    }

    public void finishClicked()
    {
        bFinishClicked = true;
        fireChangedEvent( new ChangeEvent( this ) );
    }

    public boolean isFinishClicked()
    {
        return bFinishClicked;
    }

    // ============================
    //  Methods for managing Change Events
    // ============================
    private EventListenerList getListeners()
    {
        if ( listeners == null )
        {
            listeners = new EventListenerList();
        }
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



