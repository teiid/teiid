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

package com.metamatrix.console.ui.views.users;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.JPanel;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.console.models.AuthenticationProviderManager;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

/**
 * Panel consisting of an upper portion (ItemsListerPanel) containing a list of
 * users and/or groups
 * or roles, and a lower panel with two buttons, usually labeled 'Add' and 'Remove'.
 * The entire panel is used to:
 * <PRE>
 * assign or deassign selected roles to a given user or group
 * assign or deassign selected users or groups from a given role
 * add or remove selected users or groups as a member of given group
 * add or remove given user or group as member of selected groups
 * </PRE>
 *
 * The usage depends on the class istantiating the ItemsListerPanel, which is itself
 * an abstract type.  Our class handles only the common aspects of all the
 * above functions, and does not know specifically what type of above function is
 * being performed.
 *
 * The selected items (users, groups, or roles) in the ItemsListerPanel pertain only
 * to the 'remove' button, which deassigns the items.
 * Whenever one or more items is selected on the ItemsListerPanel, the 'remove'
 * button is enabled.  If pressed, a confirmation dialog is displayed.  If
 * confirmed, a controller is notified and actually performs the deassignments.
 *
 * Assigning new items is related to the 'add' button.  When it is pressed, we do nothing
 * other than to notify a MembershipController object.
 */
public class RoleMembershipPanel extends JPanel implements GroupAccumulatorController {
	
    //Abstract panel which presents a list of groups, users, or roles, of which
    //one or more items may be selected by user:
    private GroupsAccumulatorPanel groupAccumulatorPanel;
    
    private GroupsManager groupsManager;

    //Object representing name and type (user, group, or role) of what we are
    //displaying:
    private RoleDisplay roleDisplay;


    /**
     * Constructor.  Also creates component.
     *
     * @param ent   name and type (user, group, or role) of the item we are dealing with
     * @param first Is this item the first item or second item to appear textually when composing removal confirmation message?
     * @param lp    abstract panel in which users, groups, or roles are displayed and may be selected
     * @param mc    object to be notified when user specifies an addition or removal
     * @param entrprs Is this item Enterprise, i.e. cannot be edited (buttons to enable changes should not be shown)?
     */
    public RoleMembershipPanel(RoleDisplay rd, GroupsManager gMgr) {
        super();
        groupsManager = gMgr;
        roleDisplay = rd;
        groupAccumulatorPanel = new GroupsAccumulatorPanel(new ArrayList(),this);
        
        final AuthenticationProviderManager authMgr = ModelManager.getAuthenticationProviderManager(gMgr.getConnection() );
        if(authMgr != null) {
            authMgr.addProvidersChangeListener(groupAccumulatorPanel);
        }

        init();
    }
    
    public GroupsManager getGroupsManager() {
        return this.groupsManager;
    }
    
    public void deregister() {
        final AuthenticationProviderManager authMgr = ModelManager.getAuthenticationProviderManager(groupsManager.getConnection() );
        if(authMgr != null) {
            authMgr.removeProvidersChangeListener(groupAccumulatorPanel);
        }
    }

    /**
     * Method to create the component.
     */
    private void init() {
        //Use a GridBagLayout
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        add(groupAccumulatorPanel);
        //Add upper panel
        layout.setConstraints(groupAccumulatorPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 5, 0, 5), 0, 0));

        TitledBorder tBorder;
        tBorder = new TitledBorder("Role Membership");
        setBorder(tBorder);
        
        // Initialize the groupsAccumulatorPanel with initial groups
        repopulateGroupTable();
    }
    
    /**
     * Repopulate the group table using the current RoleDisplay
     */
    private void repopulateGroupTable( ) {
        groupAccumulatorPanel.repopulateTable(getPrincipalsForRole(this.roleDisplay));
    }
    
    /**
     * Get the list of principals for the provided role
     */
    private List getPrincipalsForRole(RoleDisplay roleDisplay) {
    	List groupPrincipals = new ArrayList();
        //Call manager to get the principals for this role
        Collection /*<MetaMatrixPrincipalName>*/ rolePrincipals = Collections.EMPTY_LIST;
        try {
        	rolePrincipals = this.groupsManager.getPrincipalsForRole(roleDisplay.getName());
        } catch (AuthorizationException e) {
            //throw e;
        } catch (ComponentNotFoundException e) {
            //throw e;
        } catch (Exception e) {
            //throw new ExternalException(e);
        }
        Iterator iter = rolePrincipals.iterator();
        while(iter.hasNext()) {
        	MetaMatrixPrincipalName principalName = (MetaMatrixPrincipalName)iter.next();
        	if( principalName.getType() == MetaMatrixPrincipal.TYPE_GROUP ) {
        		groupPrincipals.add(principalName);
        	}
        }
        return groupPrincipals;
    }    

    //Method called when 'Remove' has been pressed, and user has responded 'yes'
    //to a confirmation dialog.  In removalItems, the first of each pair is the
    //user, group, or role being removed.  The second of each pair is the user
    //or group from which it is being removed.
    public boolean removeConfirmed(Component callingPanel, Collection mmPrincipalNames) {
        removeRoleFromPrincipals(mmPrincipalNames);
        return true;
    }
    
    /**
     * Method to do the requested role removals.
     */
    private void removeRoleFromPrincipals(Collection principalNames) {
        try {
            //Change to wait cursor
            StaticUtilities.startWait(this);

            //Have manager do the removal.  Can throw exceptions.
            this.groupsManager.removePrincipalsFromRole(principalNames, 
            		roleDisplay.getName());
            //Removals succeeded.  Repopulate the panel.  Will do API call to
            //get updated list of principals this role assigned to.
            //rolePanel.getListPanel().repopulatePanel(null);
        } catch (Exception e) {
            //Exception occurred.  We will simply inform user via error dialog.
            ExceptionUtility.showMessage("Remove Role From Groups", e);
        }
        finally {
            //Switch back from wait cursor
            StaticUtilities.endWait(this);
        }
    }

    /**
     * Show dialog to assign role to additional principals.
     */
    public Collection addPressed(Component callingPanel) {
        try {
            //Switch to wait cursor
            StaticUtilities.startWait(this);

            //Get the groups currently assigned to the role
            Collection /*<MetaMatrixPrincipalName>*/ principalsHavingRole =
            	this.groupsManager.getPrincipalsForRole(roleDisplay.getName());
            Collection groupsHavingRole = new ArrayList();
            Iterator iter = principalsHavingRole.iterator();
            while(iter.hasNext()) {
            	MetaMatrixPrincipalName principalName = (MetaMatrixPrincipalName)iter.next();
            	if(principalName.getType() == MetaMatrixPrincipal.TYPE_GROUP) {
            		groupsHavingRole.add(principalName);
            	}
            }
            
	        //Remove wait cursor
    	    StaticUtilities.endWait(this);

    	    // Launch the Groups Selection Wizard
            NewGroupsWizardController controller = new NewGroupsWizardController(this.groupsManager);
            List newGroups = controller.runWizard(groupsHavingRole);

            //Was anything checked?
	        if (newGroups.size() > 0) {
	            //Have manager do API call to add the principals to the
    	        //role.  Can throw exceptions.
    	        this.groupsManager.addPrincipalsToRole(newGroups, 
        	    		roleDisplay.getName());
            	//Assignments succeeded.  Repopulate the panel.  Will do API call to
                //get updated list of principals assigned to.
    	        repopulateGroupTable();
            }
		} catch (Exception e) {
        	//Exception occurred.  We will simply put up an error dialog.
            ExceptionUtility.showMessage("Assign Role to Groups", e);
		} finally {
    		//In case wait cursor did not get removed
			StaticUtilities.endWait(this);
        }
		return null;
    }

}
