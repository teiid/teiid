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

package com.metamatrix.console.ui.views.runtime;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.swing.Action;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.CompoundBorder;

import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.runtime.util.RuntimeMgmtUtils;
import com.metamatrix.console.ui.views.runtime.util.ServiceStateConstants;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public final class OperationsPanel extends JPanel
        implements ServiceStateConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static final String START_TEXT = RuntimeMgmtUtils.getString("actionStart");
    private static final String STOP_TEXT = RuntimeMgmtUtils.getString("actionStop");
    private static final String STOP_NOW_TEXT = RuntimeMgmtUtils.getString("actionStopNow");
    private static final String SHOW_ERROR_TEXT = RuntimeMgmtUtils.getString("actionShowServiceError");
    private static final String SHOW_PROCESS_TEXT = RuntimeMgmtUtils.getString("actionShowProcess");
    private static final String SHOW_QUEUES_TEXT = RuntimeMgmtUtils.getString("actionShowQueues");
    private static final String SHOW_QUEUE_TEXT = RuntimeMgmtUtils.getString("actionShowQueue");
    
    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private PanelAction actionStart = new PanelAction(START);
    private PanelAction actionStop = new PanelAction(STOP);
    private PanelAction actionStopNow = new PanelAction(STOP_NOW);
    private PanelAction actionShowQueue = new PanelAction(SHOWQUEUE);
    private PanelAction actionShowQueues = new PanelAction(SHOWQUEUES);
    private PanelAction actionShowProcess = new PanelAction(SHOWPROCESS);
    private PanelAction actionShowServiceError = new PanelAction(SHOW_SERVICE_ERROR);
    
    private MenuEntry startMenueEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, actionStart);
    private MenuEntry stopMenueEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, actionStop);
    private MenuEntry stopNowMenueEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, actionStopNow);
    private MenuEntry showQueueMenueEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, actionShowQueue);
    private MenuEntry showQueuesMenueEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, actionShowQueues);
    private MenuEntry showProcessMenueEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, actionShowProcess);
    private MenuEntry showServiceErrorMenueEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, actionShowServiceError);
    
    private ArrayList permanentActions = new ArrayList(TOTAL_OPERATIONS);
    private ArrayList actions = new ArrayList(TOTAL_OPERATIONS);
    private ButtonWidget btnShowQueue = new ButtonWidget();
    private ButtonWidget btnShowProcess = new ButtonWidget();
    private ButtonWidget btnShowQueues = new ButtonWidget();
    private ButtonWidget btnShowServiceError = new ButtonWidget();
    private JPanel pnlOpsSizer;
    
    // Object state
    private boolean disabled = false;
    private boolean enableShowQueue/*, enableShowProcess*/;
    private ServiceData  serviceData = null;
    private ProcessData  processData = null;
    private OperationsDelegate delegate;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public OperationsPanel(OperationsDelegate theDelegate) {

        delegate = theDelegate;
        TitledBorder tBorder = new TitledBorder(RuntimeMgmtUtils.getString("op.title"));
        pnlOpsSizer = new JPanel(new GridLayout(TOTAL_DISPLAYED_OPERATIONS, 1, 0, 6));
        pnlOpsSizer.setBorder(new CompoundBorder(tBorder, RuntimeMgmtUtils.EMPTY_BORDER));
        add(pnlOpsSizer);

        ButtonWidget btnStart = new ButtonWidget();
        btnStart.setPreferredSize(new Dimension(90,25));
        actionStart.addComponent(btnStart);
        pnlOpsSizer.add(btnStart);

        ButtonWidget btnStop = new ButtonWidget();
        actionStop.addComponent(btnStop);
        pnlOpsSizer.add(btnStop);

        ButtonWidget btnStopNow = new ButtonWidget();
        actionStopNow.addComponent(btnStopNow);
        pnlOpsSizer.add(btnStopNow);

        // These MenuEntries will allways appere visible
        // They will be enabled/disabled in certain contexts
        actions.add(startMenueEntry);
        actions.add(stopMenueEntry);
        actions.add(stopNowMenueEntry);
        
        // Used via actions.retainAll(permanentActions) to
        // remove all actions but these "permanent" ones
        permanentActions.add(startMenueEntry);
        permanentActions.add(stopMenueEntry);
        permanentActions.add(stopNowMenueEntry);
        
	}

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public List getActions() {
        return (List)actions.clone();
    }

    public void setActionsDisabled() {
        setEnabled(new boolean[TOTAL_OPERATIONS]);
        disabled = true;
    }

    public void setEnabled(boolean[] theEnableFlags) {
        if (!disabled) {
            setEnabled(actionStart, theEnableFlags[START_ORDINAL_POSITION]);
            setEnabled(actionStop, theEnableFlags[STOP_ORDINAL_POSITION]);
            setEnabled(actionStopNow, theEnableFlags[STOP_NOW_ORDINAL_POSITION]);
		}
    }
    
    public void setServiceData(ServiceData serviceData) {
        this.serviceData =  serviceData;
        setEnabledShowQueues(serviceData);
    }

    public void setProcessDate(ProcessData processData) {
        this.processData = processData;
    }

    public void setShowQueue(boolean showQueue) {
        enableShowQueue = showQueue;
        setEnabledShowQueues(serviceData);
    }

    public void setShowProcess(boolean showProcess) {
        setEnabledShowProcess(processData);
    }

    public void setEnabledShowQueues(ServiceData serviceData) {
        if (delegate.isServiceDisplayed(serviceData) || (!enableShowQueue)) {
            btnShowQueue.setEnabled(false);
            btnShowQueues.setEnabled(false);
            setShowQueueEnabled(false);
            setShowQueuesEnabled(false);
        } else {
            btnShowQueue.setEnabled(true);
            btnShowQueues.setEnabled(true);
            setShowQueueEnabled(true);
            setShowQueuesEnabled(true);
        }
    }

    public void setEnabledShowProcess(ProcessData processData) {
        if (delegate.isProcessDisplayed(processData)) {
            btnShowProcess.setEnabled(false);
            setShowProcessEnabled(false);
        } else {
            //Fix to defect 15626.  BWP 01/04/05
            if (!processData.isRegistered()) {
                btnShowProcess.setEnabled(false);
                setShowProcessEnabled(false);
            } else {
                btnShowProcess.setEnabled(true);
                setShowProcessEnabled(true);
            }
        }
    }

    public void setVisibleProcess(boolean visibleFlag) {
        clearOperationsPanel();

        if (visibleFlag) {
            actionShowProcess.addComponent(btnShowProcess);
            pnlOpsSizer.add(btnShowProcess);
            actions.add(showProcessMenueEntry);
        }
        pnlOpsSizer.validate();
        pnlOpsSizer.repaint();
    }

    public void setVisibleService(boolean visibleFlag, int numberOfQueues, boolean hasError) {
        clearOperationsPanel();
        if (visibleFlag) {
            if (numberOfQueues == 1) {
                if (! actions.contains(showQueueMenueEntry) ) {
                    actions.add(showQueueMenueEntry);
                }
                actionShowQueue.addComponent(btnShowQueue);
                pnlOpsSizer.add(btnShowQueue);
            } else if (numberOfQueues > 1) {
                if (! actions.contains(showQueuesMenueEntry) ) {
                    actions.add(showQueuesMenueEntry);
                }
                actionShowQueue.addComponent(btnShowQueues);
                pnlOpsSizer.add(btnShowQueues);
            } else if (hasError) {
                if (! actions.contains(showServiceErrorMenueEntry) ) {
                    actions.add(showServiceErrorMenueEntry);
                }
                actionShowServiceError.addComponent(btnShowServiceError);
                btnShowServiceError.setEnabled(true);
                setEnabled(actionShowServiceError, true);
                pnlOpsSizer.add(btnShowServiceError);
            }
        }
        pnlOpsSizer.validate();
        pnlOpsSizer.repaint();
    }

    public void setEnabled(boolean theEnabledFlag) {
        if (!disabled) {
            boolean[] enablements = new boolean[TOTAL_OPERATIONS];
            for (int i = 0; i < TOTAL_OPERATIONS; enablements[i++] = theEnabledFlag) {
                // setting all "enablements" array elements to value of "theEnabledFlag"
            }
            setEnabled(enablements);
        }
    }

    private void clearOperationsPanel() {
        actions.retainAll(permanentActions);
        
        ButtonWidget button = getButtonComponent(pnlOpsSizer, SHOW_QUEUE_TEXT);
        if ( button != null ) {
            actionShowQueue.removeComponent(btnShowQueue);
            pnlOpsSizer.remove(btnShowQueue);
        }
        
        button = getButtonComponent(pnlOpsSizer, SHOW_QUEUES_TEXT);
        if ( button != null ) {
            actionShowQueues.removeComponent(btnShowQueues);
            pnlOpsSizer.remove(btnShowQueues);
        }
        
        button = getButtonComponent(pnlOpsSizer, SHOW_PROCESS_TEXT);
        if ( button != null ) {
            actionShowProcess.removeComponent(btnShowProcess);
            pnlOpsSizer.remove(btnShowProcess);
        }
        
        button = getButtonComponent(pnlOpsSizer, SHOW_ERROR_TEXT);
        if ( button != null ) {
            actionShowServiceError.removeComponent(btnShowServiceError);
            pnlOpsSizer.remove(btnShowServiceError);
        }
        pnlOpsSizer.validate();
        pnlOpsSizer.repaint();
    }

    private void setShowQueueEnabled(boolean theEnableFlags) {
        setEnabled(actionShowQueue, theEnableFlags);
    }

    private void setShowQueuesEnabled(boolean theEnableFlags) {
        setEnabled(actionShowQueues, theEnableFlags);
    }

    private void setShowProcessEnabled(boolean theEnableFlags) {
        setEnabled(actionShowProcess, theEnableFlags);
    }

    private void setEnabled(Action theAction, boolean theEnabledFlag) {
        if (!disabled) {
            if (theEnabledFlag) {
                if (!theAction.isEnabled()) {
                    theAction.setEnabled(true);
                }
            } else {
                if (theAction.isEnabled()) {
                    theAction.setEnabled(false);
                }
            }
        }
    }

    /** 
     * Helper to get a particular button from the container.
     * @param pnlOpsSizer2 the container.
     * @param buttonText 
     * @return the button with "buttonText" or null if not contained.
     * @since 4.4
     */
    private ButtonWidget getButtonComponent(JPanel pnlOpsSizer2, String buttonText) {
        ButtonWidget theButton = null;
         Component[] components = pnlOpsSizer2.getComponents();
        for (int i = 0; i < components.length; i++) {
            Component aComponent = components[i];
            if ( aComponent instanceof ButtonWidget && ((ButtonWidget)aComponent).getText().equals(buttonText)) {
                theButton = (ButtonWidget)aComponent;
                break;
            }
        }
        return theButton;
    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {

        public PanelAction(int theType) {
            super(theType);
            
            switch (theType) {
                case START:
                    putValue(NAME, START_TEXT);
                    putValue(SHORT_DESCRIPTION,  RuntimeMgmtUtils.getString("actionStart.tip"));
                    break;
                case STOP:
                    putValue(NAME, STOP_TEXT);
                    putValue(SHORT_DESCRIPTION, RuntimeMgmtUtils.getString("actionStop.tip"));
                    break;
                case STOP_NOW:
                    putValue(NAME, STOP_NOW_TEXT);
                    putValue(SHORT_DESCRIPTION, RuntimeMgmtUtils.getString("actionStopNow.tip"));
                    break;
                case SHOWQUEUE:
                    putValue(NAME, SHOW_QUEUE_TEXT);
                    break;
                case SHOWQUEUES:
                    putValue(NAME, SHOW_QUEUES_TEXT);
                    break;
                case SHOWPROCESS:
                    putValue(NAME, SHOW_PROCESS_TEXT);
                    break;
                case SHOW_SERVICE_ERROR:
                    putValue(NAME, SHOW_ERROR_TEXT);
                    break;
                default:
                    throw new IllegalArgumentException("The action type <" + theType + "> is invalid.");
            }
        }
        
        public void actionImpl(ActionEvent theEvent)
            throws ExternalException {

            switch (super.type) {
                case START:
                    delegate.startOperation();
                    break;
                case STOP:
                    delegate.stopOperation();
                    break;
                case STOP_NOW:
                    delegate.stopNowOperation();
                    break;
                case SHOWQUEUE:
                case SHOWQUEUES:
                    //create pop-up frame, update main panel, then show pop-up frame
                    QueueStatisticsFrame qsFrame = delegate.startShowQueue(serviceData);
                    setEnabledShowQueues(serviceData);
                    if (qsFrame != null) {
                        qsFrame.show();
                    }
                    break;
                case SHOWPROCESS:
                    //create pop-up frame, update main panel, then show pop-up frame
                    VMStatisticsFrame vmsFrame = delegate.startShowProcess(processData);
                    setEnabledShowProcess(processData);
                    if (vmsFrame != null) {
                        vmsFrame.show();
                    }
                    break;
                case SHOW_SERVICE_ERROR:
                    delegate.showServcieError();
                    break;
                default:
                    // none
            }
        }

        protected void handleError(Exception theException) {
            String emsg = theException.toString();
            String exceptionLine;
            boolean showError = true;
            StringTokenizer st = new StringTokenizer(emsg, "\n");
            while (st.hasMoreTokens()) {
                exceptionLine = st.nextToken();
                if (exceptionLine.indexOf(
                        "Is last instance of an essential service") >= 0) {
                    showError = false;
                    break;
                }
            }
            if ((type == STOP) && (!showError)) {
                JOptionPane.showMessageDialog(ConsoleMainFrame.getInstance(),
                        "Cannot stop essential service", "Information",
                        JOptionPane.INFORMATION_MESSAGE);
			} else if ((type == STOP_NOW) && (!showError)) {
                JOptionPane.showMessageDialog(ConsoleMainFrame.getInstance(),
                        "Cannot do \"Stop Now\" for essential service", "Information",
                        JOptionPane.INFORMATION_MESSAGE);
            }
            if (showError) {
                super.handleError(theException);
            }
        }
    }     
}