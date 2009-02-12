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

package com.metamatrix.console.ui.views.entitlements;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import com.metamatrix.console.models.AuthenticationProviderManager;
import com.metamatrix.console.models.EntitlementManager;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.NoMinTextFieldWidget;
import com.metamatrix.console.ui.util.RepaintController;
import com.metamatrix.console.ui.views.users.GroupAccumulatorController;
import com.metamatrix.console.ui.views.users.GroupsAccumulatorPanel;
import com.metamatrix.console.ui.views.users.NewGroupsWizardController;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.admin.api.PermissionTreeView;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

public class EntitlementDetailPanel extends BasePanel
        implements ButtonsStateController, NotifyOnExitConsole, PrincipalChangeListener, GroupAccumulatorController {
    public final static Color VALUES_COLOR = Color.black;
    public final static int INITIAL_LIST_SIZES = 50;

    private EntitlementsDataInterface dataSource;
//    private RepaintController repaintController;
    private boolean showingMetaMatrix;
    private boolean showingEnterprise;
    private boolean canModify;
    private DataNodeAuthorizationsControl authorizationsPanel = null;
    private GroupsAccumulatorPanel groupsAccumulator = null;
    private ButtonWidget applyButton;
    private ButtonWidget resetButton;
    private TextFieldWidget vdbNameValue;
    private TextFieldWidget vdbVersionValue;
    private TextFieldWidget entitlementNameValue;
    private JTextArea entitlementDescription;
    private JTabbedPane tabbedPane;
    private JPanel buttonsPanel;
    private JPanel textPanel;
    private GridBagLayout layout;
    private java.util.List /*<String>*/ allAddedEnterpriseGroups = new ArrayList(1);
    private java.util.List /*<String>*/ allRemovedEnterpriseGroups = new ArrayList(1);
    private String existingDescription = ""; //$NON-NLS-1$
    private AuthorizationPolicyID policyID;
    private AuthorizationPolicy policy;
    private String vdbName;
    private int vdbVersion;
    private String entitlementName;
    private String entitlementDesc;
    private PermissionTreeView treeView;
    private JScrollPane authPanelScrollPane;
    private java.util.List /*<String>*/ existingEnterpriseGroups = null;
    
    private EntitlementManager manager;
    private GroupsManager groupsManager;

    public EntitlementDetailPanel(EntitlementsDataInterface dataSrc,
            RepaintController rc, boolean showMetaMatrix, boolean showEnterprise,
            boolean modifiable, EntitlementManager mgr, GroupsManager userMgr) {
        super();
        dataSource = dataSrc;
//        repaintController = rc;
        showingMetaMatrix = showMetaMatrix;
        showingEnterprise = showEnterprise;
        canModify = modifiable;
        manager = mgr;
        groupsManager = userMgr;
        init();
        
        final AuthenticationProviderManager authMgr = ModelManager.getAuthenticationProviderManager(userMgr.getConnection());
        if(authMgr != null) {
            authMgr.addProvidersChangeListener(groupsAccumulator);
        }
        
        groupsManager.addPrincipalChangeListener(this);
    }

    private void init() {
        layout = new GridBagLayout();
        setLayout(layout);
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
        buttonsPanel = new JPanel();
        GridBagLayout bl = new GridBagLayout();
        buttonsPanel.setLayout(bl);
        buttonsPanel.add(applyButton);
        buttonsPanel.add(resetButton);
        bl.setConstraints(applyButton, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 15), 0, 0));
        bl.setConstraints(resetButton, new GridBagConstraints(1, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 15, 5, 5), 0, 0));
        this.add(buttonsPanel);
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(0, 0, 0, 0), 0, 0));
        buttonsPanel.setVisible(canModify);

        textPanel = new JPanel();
        GridBagLayout tl = new GridBagLayout();
        textPanel.setLayout(tl);
        LabelWidget nameLabel = new LabelWidget("Name:"); //$NON-NLS-1$
        textPanel.add(nameLabel);
        tl.setConstraints(nameLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        entitlementNameValue = new NoMinTextFieldWidget(75);
        entitlementNameValue.setEditable(false);
        entitlementNameValue.setForeground(VALUES_COLOR);
        textPanel.add(entitlementNameValue);
        tl.setConstraints(entitlementNameValue, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        LabelWidget descriptionLabel = new LabelWidget("Description:"); //$NON-NLS-1$
        textPanel.add(descriptionLabel);
        tl.setConstraints(descriptionLabel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        DefaultTextFieldModel document = new DefaultTextFieldModel();
        document.setMaximumLength(NewEntitlementNamePanel.MAX_DESCRIPTION_LENGTH);
        entitlementDescription = new JTextArea(document);
        entitlementDescription.setColumns(40);
        entitlementDescription.setRows(4);
        entitlementDescription.setPreferredSize(new Dimension(150, 68));
        entitlementDescription.setLineWrap(true);
        entitlementDescription.setWrapStyleWord(true);
        entitlementDescription.setEditable(false);
        entitlementDescription.setText(""); //$NON-NLS-1$
        entitlementDescription.addKeyListener(new KeyListener() {
            public void keyPressed(KeyEvent ev) {
            }
            public void keyReleased(KeyEvent ev) {
                setButtonStates();
            }
            public void keyTyped(KeyEvent ev) {
                setButtonStates();
            }
        });
        textPanel.add(entitlementDescription);
        tl.setConstraints(entitlementDescription, new GridBagConstraints(1, 1, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(10, 10, 10, 10), 0, 0));
        LabelWidget vdbNameLabel = new LabelWidget("VDB Name:"); //$NON-NLS-1$
        textPanel.add(vdbNameLabel);
        tl.setConstraints(vdbNameLabel, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        vdbNameValue = new NoMinTextFieldWidget(50);
        vdbNameValue.setEditable(false);
        vdbNameValue.setForeground(VALUES_COLOR);
        textPanel.add(vdbNameValue);
        tl.setConstraints(vdbNameValue, new GridBagConstraints(1, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        LabelWidget vdbVersLabel = new LabelWidget("VDB Version:"); //$NON-NLS-1$
        textPanel.add(vdbVersLabel);
        tl.setConstraints(vdbVersLabel, new GridBagConstraints(0, 3, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        vdbVersionValue = new NoMinTextFieldWidget(5);
        vdbVersionValue.setEditable(false);
        vdbVersionValue.setForeground(VALUES_COLOR);
        textPanel.add(vdbVersionValue);
        tl.setConstraints(vdbVersionValue, new GridBagConstraints(1, 3, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));

        authorizationsPanel = new DataNodeAuthorizationsControl(this, canModify,
        		false, true, true);

        groupsAccumulator = new GroupsAccumulatorPanel(new ArrayList(),this);

        tabbedPane = new JTabbedPane();
        this.add(tabbedPane);
        layout.setConstraints(tabbedPane, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
        tabbedPane.addTab("Details", textPanel); //$NON-NLS-1$
        tabbedPane.addTab("Authorizations", authorizationsPanel); //$NON-NLS-1$
        tabbedPane.addTab("Groups", groupsAccumulator); //$NON-NLS-1$

        resetButton.setEnabled(false);
        applyButton.setEnabled(false);
    }

    public String getEntitlementName() {
        return entitlementName;
    }

    public String getVDBName() {
        return vdbName;
    }

    public int getVDBVersion() {
        return vdbVersion;
    }

    public boolean isShowingMetaMatrix() {
        return showingMetaMatrix;
    }

    public boolean isShowingEnterprise() {
        return showingEnterprise;
    }

    private void applyPressed() {
        int value = -1;
        boolean resettingDataNodesTreeScrollPosit = false;
        if (authPanelScrollPane != null) {
            JScrollBar vertScrollBar = authPanelScrollPane.getVerticalScrollBar();
            if (vertScrollBar != null) {
                value = vertScrollBar.getValue();
                resettingDataNodesTreeScrollPosit = true;
            }
        }
        doChanges();
        forceRepaint();
        if (resettingDataNodesTreeScrollPosit) {
            final int val = value;
            SwingUtilities.invokeLater(new Runnable() {
                public void run() {
                    authPanelScrollPane.getVerticalScrollBar().setValue(val);
                }
            });
        }
    }

    private void resetPressed() {
        clearChanges();
        forceRepaint();
    }
    
    public GroupsManager getGroupsManager() {
        return this.groupsManager;
    }

    public void forceRepaint() {
        ConsoleMainFrame.getInstance().repaintNeeded();
    }

    public AuthorizationPolicyID getPolicyID() {
        return policyID;
    }

    public void populate(EntitlementInfo ent, boolean repopulatingSameEntitlement,
            boolean canModifyEntitlements) {
        if (ent != null) {
            policyID = ent.getPolicyID();
            policy = ent.getPolicy();
            vdbName = ent.getVDBName();
            vdbVersion = ent.getVDBVersion();
            entitlementName = ent.getEntitlementName();
            entitlementDesc = ent.getEntitlementDescription();
            treeView = ent.getTreeView();
            allAddedEnterpriseGroups = new ArrayList(INITIAL_LIST_SIZES);
            allRemovedEnterpriseGroups = new ArrayList(INITIAL_LIST_SIZES);
            vdbVersionValue.setText((new Integer(ent.getVDBVersion())).toString());
        } else {
            policyID = null;
            policy = null;
            vdbName = ""; //$NON-NLS-1$
            vdbVersion = -1;
            entitlementName = ""; //$NON-NLS-1$
            entitlementDesc = ""; //$NON-NLS-1$
            treeView = null;
            vdbVersionValue.setText(""); //$NON-NLS-1$
        }


        authPanelScrollPane = null;
        vdbNameValue.setText(vdbName);
        entitlementNameValue.setText(entitlementName);
        entitlementDescription.setText(entitlementDesc);

        authorizationsPanel.setTreeView(treeView, repopulatingSameEntitlement,
                canModifyEntitlements);

        //Store the initial values.  If "reset" pressed, we have to reset to these
        if (ent != null) {
            existingDescription = ent.getEntitlementDescription();
            existingEnterpriseGroups = ent.getEnterpriseGroups();
        } else {
            existingDescription = ""; //$NON-NLS-1$
            existingEnterpriseGroups = new ArrayList();
        }

        //This has better set button states to disabled or we have an error
        setButtonStates();
        entitlementDescription.setEditable((canModify && (ent != null)));

    	this.groupsAccumulator.repopulateTable(this.existingEnterpriseGroups);

        forceRepaint();
    }

    public void clear() {
        populate(null, false, canModify);
    }

    public void postRealize() {
    }

    public void permissionsChanged() {
        setButtonStates();
    }

    public void principalsChanged() {
        setButtonStates();
    }

    private void setButtonStates() {
        boolean enabling = anyChangeMade();
        resetButton.setEnabled(enabling);
        applyButton.setEnabled(enabling);
    }

    private boolean anyChangeMade() {
        boolean change = false;
        if (!entitlementDescription.getText().equals(existingDescription)) {
            change = true;
        } else {
            if ((allRemovedEnterpriseGroups.size() > 0) ||
                (allAddedEnterpriseGroups.size() > 0)) {
                change = true;
            } else {
                if (anyDataNodesPermissionsChangeMade()) {
                    change = true;
                }
            }
        }
        return change;
    }

    public boolean havePendingChanges() {
        boolean changed = false;
        if (policy != null) {
            changed = anyChangeMade();
        }
        return changed;
    }

    public boolean finishUp() {
        boolean stayingHere = false;
        String msg = "Save changes to role \"" + entitlementName + "\"?"; //$NON-NLS-1$ //$NON-NLS-2$
        int response = DialogUtility.showPendingChangesDialog(msg, 
        		manager.getConnection().getURL(),
        		manager.getConnection().getUser());
        switch (response) {
            case DialogUtility.YES:
                doChanges();
                stayingHere = false;
                break;
            case DialogUtility.NO:
                stayingHere = false;
                break;
            case DialogUtility.CANCEL:
                stayingHere = true;
                break;
        }
        return (!stayingHere);
    }

    private boolean anyDataNodesPermissionsChangeMade() {
        return authorizationsPanel.anyAuthorizationsChangesMade();
    }
    
    /**
     * called when 'Add' button has been pressed.
     */
    public Collection addPressed(Component callingPanel) {
        try {
    	    // Launch the Groups Selection Wizard
            NewGroupsWizardController controller = new NewGroupsWizardController(this.groupsManager);
            List newGroups = controller.runWizard(getExistingPlusAddedGroups());

            // Update the Added and Removed Groups lists
            for(int i=0; i<newGroups.size(); i++) {
            	Object group = newGroups.get(i);
            	if(allRemovedEnterpriseGroups.contains(group)) {
            		allRemovedEnterpriseGroups.remove(group);
            	}
            	if(!allAddedEnterpriseGroups.contains(group)) {
            		allAddedEnterpriseGroups.add(group);
            	}
            }
            // Repopulate the accumulator table
        	this.groupsAccumulator.repopulateTable(getExistingPlusAddedGroups());

		} catch (Exception e) {
        	//Exception occurred.  We will simply put up an error dialog.
            ExceptionUtility.showMessage("Assign Role to Groups", e);
		} finally {
    		//In case wait cursor did not get removed
			StaticUtilities.endWait(this);
        }
    	principalsChanged();
		return null;
    }

    //Method called when 'Remove' has been pressed, and user has responded 'yes'
    //to a confirmation dialog.  In removalItems, the first of each pair is the
    //user, group, or role being removed.  The second of each pair is the user
    //or group from which it is being removed.
    public boolean removeConfirmed(Component callingPanel, Collection removedMMPrincipals) {
        // Update the Added and Removed Groups lists
    	Iterator iter = removedMMPrincipals.iterator();
    	while(iter.hasNext()) {
    		Object group = iter.next();
        	if(allAddedEnterpriseGroups.contains(group)) {
        		allAddedEnterpriseGroups.remove(group);
        	}
        	if(!allRemovedEnterpriseGroups.contains(group)) {
        		allRemovedEnterpriseGroups.add(group);
        	}
    	}
    	principalsChanged();
    	return true;
    }
    
    private List getExistingPlusAddedGroups() {
    	if(this.existingEnterpriseGroups!=null) {
	    	List existingPlusAdded = new ArrayList(this.existingEnterpriseGroups);
	    	Iterator iter = this.allAddedEnterpriseGroups.iterator();
	    	while(iter.hasNext()) {
	    		Object group = iter.next();
	    		existingPlusAdded.add(group);
	    	}
	    	return existingPlusAdded;
    	}
    	return Collections.EMPTY_LIST;
    }

    private void doChanges() {
    	StaticUtilities.startWait(ViewManager.getMainFrame());
		String displayedDescription = entitlementDescription.getText();
        if (!displayedDescription.equals(existingDescription)) {
			if ( displayedDescription == null ) {
                displayedDescription = ""; //$NON-NLS-1$
            }

        }
        boolean continuing = true;
        try {
            java.util.List /*<DataNodePermissionsWithNodeName>*/ changedNodes =
                    authorizationsPanel.nodesWithAuthorizationChanges();
            manager.changeAPolicy((PermissionDataNodeTreeView)treeView, policy, 
            		displayedDescription, allAddedEnterpriseGroups, 
                    allRemovedEnterpriseGroups, changedNodes);
        } catch (Exception ex) {
        	StaticUtilities.endWait(ViewManager.getMainFrame());
            ExceptionUtility.showMessage("Modify Role", ex); //$NON-NLS-1$
            continuing = false;
        }
        if (continuing) {
            //Now we will repopulate from the entitlement as stored in the DB.
            try {
                EntitlementInfo info = dataSource.getEntitlementInfo(
                        entitlementName, vdbName, vdbVersion);
                try {
                    populate(info, true, canModify);
                } catch( Exception e ) {
                	StaticUtilities.endWait(ViewManager.getMainFrame());
                    ExceptionUtility.showMessage( "Failed populating the detail panel ", e ); //$NON-NLS-1$
                    continuing = false;
                } finally {
                    StaticUtilities.endWait(ViewManager.getMainFrame());
                }
                forceRepaint();
                //This had better set the buttons to disabled or we have a problem
                setButtonStates();
            } catch (Exception ex) {
            	StaticUtilities.endWait(ViewManager.getMainFrame());
                ExceptionUtility.showMessage("Retrieve modified role", ex); //$NON-NLS-1$
                continuing = false;
            }
        }
        if (continuing) {
        	StaticUtilities.endWait(ViewManager.getMainFrame());
        }
    }

    private void clearChanges() {
    	// Clear the added and removed groups
        allAddedEnterpriseGroups.clear();
        allRemovedEnterpriseGroups.clear();
        
        // Repopulate the groupsTable with the original groups
    	this.groupsAccumulator.repopulateTable(this.existingEnterpriseGroups);
    	// Reset the description
        entitlementDescription.setText(existingDescription);
        // Reset the authorizations
        authorizationsPanel.reset();
        
        forceRepaint();
        //This had better set the buttons to disabled or we have a problem.
        setButtonStates();
    }
    
    // Respond to changes added or Removed Groups
    public void principalCreated(MetaMatrixPrincipalName princ) {
//      if (princ.getType() == MetaMatrixPrincipal.TYPE_USER) {
//          addNewMetaMatrixUser(princ.getName());
//      } else {
//          addNewMetaMatrixGroup(princ.getName());
//      }
  }

  public void principalDeleted(MetaMatrixPrincipalName princ) {
//      if (princ.getType() == MetaMatrixPrincipal.TYPE_USER) {
//          deleteMetaMatrixUser(princ.getName());
//      } else {
//          deleteMetaMatrixGroup(princ.getName());
//      }
  }

    public void paint(Graphics g) {
        super.paint(g);
    }
}//end EntitlementDetailPanel
