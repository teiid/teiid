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

//#############################################################################
package com.metamatrix.console.ui.views.deploy;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.util.*;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.KeyStroke;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.*;

public final class CreatePSCPanel
    extends ConfirmationPanel
    implements ActionListener,
               DocumentListener {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static final KeyStroke ENTER_RELEASED =
        KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0, true);

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TextFieldWidget txf;
    private ButtonWidget btnCreate;
    
	private AccumulatorPanel pnlAssignments; 
	
	private ProductServiceConfig pscDef;   
	private List serviceNames;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public CreatePSCPanel(    	
        String theMessageId,
        String theIconId,
        String theLabelId,
        String theNameTypeId,
        ProductServiceConfig psc, 
        List serviceNames) {

        super(theMessageId, theIconId, "rp.btnCreate", "rp.btnCancel"); //$NON-NLS-1$ //$NON-NLS-2$
        this.pscDef = psc;
        this.serviceNames = serviceNames;
        JPanel mainPanel = new JPanel(new BorderLayout());

        JPanel pnl = new JPanel();
        JLabel lbl = new LabelWidget(DeployPkgUtils.getString(theLabelId));
        pnl.add(lbl);
        txf = DeployPkgUtils.createTextField(theNameTypeId);
        txf.getDocument().addDocumentListener(this);
        txf.registerKeyboardAction(this, ENTER_RELEASED, WHEN_FOCUSED);
        pnl.add(txf);
        mainPanel.add(pnl, BorderLayout.NORTH);
        mainPanel.add(createAssignmentPanel(), BorderLayout.CENTER);
        addContent(mainPanel);
        btnCreate = getAcceptButton();
        btnCreate.setEnabled(false);
    }
    
               
    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public void actionPerformed(ActionEvent theEvent) {
        btnCreate.doClick();
    }

    public void changedUpdate(DocumentEvent theEvent) {
        if ((txf.getText().length() > 0) && !btnCreate.isEnabled()) {
            btnCreate.setEnabled(true);
        }
        else if ((txf.getText().length() == 0) && btnCreate.isEnabled()) {
            btnCreate.setEnabled(false);
        }
    }

    public String getName() {
        return txf.getText();
    }

    public void insertUpdate(DocumentEvent theEvent) {
        changedUpdate(theEvent);
    }

    public void removeUpdate(DocumentEvent theEvent) {
        changedUpdate(theEvent);
    }
    
       
	private JPanel createAssignmentPanel() {
 
            JPanel pnl = new JPanel(new GridLayout(1, 1));
            List currNames = null;
            // if this is for an existing psc the present the existing
            // services in the psc
            if (pscDef != null) {
	            Iterator itIds = pscDef.getServiceComponentDefnIDs().iterator();
            
    	        currNames = new ArrayList(pscDef.getServiceComponentDefnIDs().size());
 			
        	    while(itIds.hasNext()){
            		ComponentDefnID id = (ComponentDefnID) itIds.next();
            		currNames.add(id);	            	
	            }
            } else {
            	currNames = Collections.EMPTY_LIST;
            }
            
           
      		if (serviceNames == null) {
      			Exception theException = new Exception("Null ServiceNames"); //$NON-NLS-1$
                theException.printStackTrace();
                LogManager.logError(
                    LogContexts.USERS,
                    theException,
                    "Error calling UserManager.getRoles()"); //$NON-NLS-1$
                    return pnl;
      		}
        		
      		   Collections.sort(serviceNames);
                if (!currNames.isEmpty()) {
	                Collections.sort(currNames);
                }
                pnlAssignments = new AccumulatorPanel(serviceNames);
                pnlAssignments.getAcceptButton().setVisible(false);
                pnlAssignments.getCancelButton().setVisible(false);
                pnlAssignments.setAllowsReorderingValues(false);
                pnlAssignments.setInitialValues(currNames);
                pnlAssignments.setMinimumValuesAllowed(
                    DeployPkgUtils.getInt("rp.minselected", 0)); //$NON-NLS-1$
                pnlAssignments.remove(pnlAssignments.getNavigationBar());
                pnlAssignments.getAvailableValuesHeader()
                        .setText(
                            DeployPkgUtils.getString(
                                "rp.avail.hdr")); //$NON-NLS-1$
                pnlAssignments.getValuesHeader()
                        .setText(
                            DeployPkgUtils.getString(
                                "rp.select.hdr")); //$NON-NLS-1$
                pnl.add(pnlAssignments);
            return pnl;
    }


    public List getSelectedServices() {
        return pnlAssignments.getValues();
    }


}
