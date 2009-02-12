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

package com.metamatrix.console.ui.layout;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.Action;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.ExceptionUtility;

public class ConsoleMenuBar extends JMenuBar {
    public final static String FILE_MENU_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.fileHeader"); //$NON-NLS-1$
    public final static String PREFERENCES_MENU_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.preferencesHeader"); //$NON-NLS-1$
    public final static String CONNECTIONS_MENU_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.connectionsHeader"); //$NON-NLS-1$
    public final static String VIEW_MENU_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.viewHeader"); //$NON-NLS-1$
    public final static String PANELS_MENU_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.panelsHeader"); //$NON-NLS-1$
    public final static String ACTIONS_MENU_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.actionsHeader"); //$NON-NLS-1$
    public final static String HELP_MENU_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.helpHeader"); //$NON-NLS-1$

    public final static String EXIT_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.exitHeader"); //$NON-NLS-1$
    public final static String URLS_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.urlsHeader"); //$NON-NLS-1$
    public final static String LOGGING_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.logPrefsHeader"); //$NON-NLS-1$
    public final static String REFRESH_RATES_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.refreshRatesHeader"); //$NON-NLS-1$
    public final static String REFRESH_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.refreshHeader"); //$NON-NLS-1$
    public final static String ABOUT_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.aboutHeader"); //$NON-NLS-1$

    public final static String PRINT_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.printHeader"); //$NON-NLS-1$
    
    public final static String ADD_CONNECTION_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.addConnectionHeader"); //$NON-NLS-1$
    public final static String REMOVE_CONNECTION_HEADER = ConsolePlugin.Util.getString("ConsoleMenuBar.remConnectionHeader"); //$NON-NLS-1$
 
    private JMenu fileMenu = null;
    private JMenu preferencesMenu = null;
    private JMenu connectionsMenu = null;
    private JMenu viewMenu = null;
    private JMenu actionsMenu = null;
    private JMenu helpMenu = null;

    private static ConsoleMenuBar theMenuBar = null;
    private static ConsoleMenuBarListener theListener = null;

// Defect 6237: Remove useless "Print" option from file menu
//    private Action actPrintDefault          = null;
    private Action actRefreshDefault        = null;

	private ConsoleMenuBar() {
        super();
        init();
    }

    public static ConsoleMenuBar getInstance() {
        if (theMenuBar == null) {
            theMenuBar = new ConsoleMenuBar();
        }
        return theMenuBar;
    }

    public static void setTheListener(ConsoleMenuBarListener listener) {
        theListener = listener;
    }

    private void init() {
        // create the menus
        addFileMenu();
        addPreferencesMenu();
        addConnectionsMenu();
        addViewMenu();
        addActionsMenu();
        addHelpMenu();
    }

    public ConsoleMenuBarListener getTheListener() {
        return ConsoleMenuBar.theListener;
    }

	public void setDefaultRefreshEnabled(boolean flag) {
		actRefreshDefault.setEnabled(flag);
	}
	
    private void addFileMenu() {
		fileMenu = new JMenu(FILE_MENU_HEADER);
		ExitAction exitAction = new ExitAction(this);
		replaceMenuItem(MenuEntry.FILE_EXIT_MENUITEM, exitAction, fileMenu);
		this.add(fileMenu);
    }

    private void addPreferencesMenu() {
        preferencesMenu = new JMenu(PREFERENCES_MENU_HEADER);
        URLsAction urlsAction = new URLsAction(this);
        replaceMenuItem(MenuEntry.PREFS_URLS_MENUITEM, urlsAction, 
        		preferencesMenu);
        RefreshRatesAction refreshRatesAction = new RefreshRatesAction(this);
        replaceMenuItem(MenuEntry.PREFS_REFRESH_RATES_MENUITEM,
        		refreshRatesAction, preferencesMenu);
        LoggingPreferencesAction loggingPreferencesAction = 
        		new LoggingPreferencesAction(this);
        replaceMenuItem(MenuEntry.PREFS_LOGGING_MENUITEM,
        		loggingPreferencesAction, preferencesMenu);
        this.add(preferencesMenu);
    }

	private void addConnectionsMenu() {
		connectionsMenu = new JMenu(CONNECTIONS_MENU_HEADER);
		AddConnectionAction addConnectionAction = new AddConnectionAction(
				this);
		replaceMenuItem(MenuEntry.CONNECTIONS_ADD_CONN_MENUITEM,
				addConnectionAction, connectionsMenu);
		RemoveConnectionAction removeConnectionAction = 
				new RemoveConnectionAction(this);
		replaceMenuItem(MenuEntry.CONNECTIONS_REMOVE_CONN_MENUITEM,
				removeConnectionAction, connectionsMenu);
		setRemoveConnectionEnabled(false);
		this.add(connectionsMenu);
	}
		
    private void addViewMenu() {
		viewMenu = new JMenu(VIEW_MENU_HEADER);
        actRefreshDefault = new RefreshAction(this);
        replaceMenuItem(MenuEntry.VIEW_REFRESH_MENUITEM, actRefreshDefault,
        		viewMenu);
        actRefreshDefault.setEnabled(false);
		this.add(viewMenu);
    }

	private void addActionsMenu() {
        actionsMenu = new JMenu(ACTIONS_MENU_HEADER);
        actionsMenu.setEnabled(false);
        this.add(actionsMenu);
    }

    private void addHelpMenu() {
        helpMenu = new JMenu(HELP_MENU_HEADER);
        AboutAction aboutAction = new AboutAction(this);
        replaceMenuItem(MenuEntry.HELP_ABOUTTHECONSOLE_MENUITEM, aboutAction,
        		helpMenu);
		this.add(helpMenu);
    }

	public void setRemoveConnectionEnabled(boolean flag) {
		//Find the remove-connection menu item
		int i = 0;
		JMenuItem matchItem = null;
		while (matchItem == null) {
			JMenuItem item = connectionsMenu.getItem(i);
			if (item.getText().equals(REMOVE_CONNECTION_HEADER)) {
				matchItem = item;
			} else {
				i++;
			}
		}
		matchItem.setEnabled(flag);
	}
	
    public void emptyTheActionsMenu() {
        actionsMenu.setPopupMenuVisible(false);
        actionsMenu.removeAll();
        actionsMenu.setEnabled(false);
    }

    public JMenuItem[] addActions(Action[] actions) {
        JMenuItem[] items = null;
        if (actions.length > 0) {
            actionsMenu.setEnabled(true);
            items = new JMenuItem[actions.length];
            for (int i = 0; i < actions.length; i++) {
                items[i] = actionsMenu.add(actions[i]);
            }
        } else {
            items = new JMenuItem[] {};
        }
        return items;
    }

    public JMenuItem[] addActions(java.util.List /*<Action>*/ actions) {
		JMenuItem[] items = null;
        int numActions = actions.size();
        if (numActions > 0) {
            actionsMenu.setEnabled(true);
            items = new JMenuItem[numActions];
            Iterator it = actions.iterator();
            for (int i = 0; it.hasNext(); i++) {
                items[i] = actionsMenu.add((Action)it.next());
            }
        } else {
            items = new JMenuItem[] {};
        }
        return items;
    }

    public JMenuItem[] addActionsFromMenuEntryObjects(
    		List /*MenuEntry*/ lstMenuEntries) {

        /*
          The List contains MenuEntry objects whose IDs are constants that
          refer to Menus and 'menu objects' which are one of:
            a) Actions
            b) MenuItems
            c) Menus (future)
        */
        restoreDefaultOverrideableMenuItems();

        ArrayList arylEntriesForActionsMenu     = new ArrayList();

        MenuEntry meEntry   = null;
        Iterator itEntries = lstMenuEntries.iterator();

        while (itEntries.hasNext()) {
            meEntry = (MenuEntry)itEntries.next();

            if (meEntry.getID().equals(MenuEntry.ACTION_MENUITEM)) {
                arylEntriesForActionsMenu.add(meEntry);
            } else if (meEntry.getID().equals(MenuEntry.SEPARATOR_LIT)) {
                arylEntriesForActionsMenu.add(meEntry);
            } else {
                addToOtherMenus(meEntry);
            }
        }

        if (arylEntriesForActionsMenu.size() > 0) {
            return addMenuEntriesToActionsMenu(arylEntriesForActionsMenu);
        }
        return null;
    }

    private void restoreDefaultOverrideableMenuItems() {
        // view/refresh
		replaceMenuItem(MenuEntry.VIEW_REFRESH_MENUITEM, actRefreshDefault,
				viewMenu);
	}

	private void addToOtherMenus(MenuEntry meEntry) {
    	// assume for now that we only have actions, not menuitems
        JMenu theMenu       = null;
        String sID          = meEntry.getID();
        if (sID.equals(MenuEntry.FILE_PRINT_MENUITEM)) {
            theMenu = fileMenu;
        } else if (sID.equals(MenuEntry.VIEW_REFRESH_MENUITEM)) {
            theMenu = viewMenu;
        } else if (sID.equals(MenuEntry.SEPARATOR_LIT)) {
            return;
        } else {
            ExceptionUtility
                .showMessage("Bad MenuEntry ID: " + sID, new Exception("")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        replaceMenuItem(meEntry.getID(), meEntry.getAction(), theMenu);
    }

    private void replaceMenuItem(String sID, Action actNewAction, JMenu mnu) {
		// walk the menuitems in this menu:
		JMenuItem mniTemp       = null;
        int iMenuItems          = mnu.getItemCount();
        AbstractPanelAction apaAction   = null;

		boolean replaced = false;
        for(int ix = 0; ix < iMenuItems; ix++) {
            mniTemp = mnu.getItem(ix);
            if (mniTemp == mnu) {
                // skip it, not a real menuitem
            } else {
                // if it is the one we want:
                if (mniTemp.getName().equals(sID)) {
                    mnu.remove(ix);
                    mniTemp = mnu.insert(actNewAction, ix);
                    mniTemp.setName(sID);

                    if (actNewAction instanceof AbstractPanelAction) {
                        apaAction = (AbstractPanelAction)actNewAction;
                        apaAction.addComponent(mniTemp);
                    }
					replaced = true;
                    break;
                }
            }
        }
        if (!replaced) {
        	//Just add the item
        	mniTemp = mnu.add(actNewAction);
        	mniTemp.setName(sID);
        	if (actNewAction instanceof AbstractPanelAction) {
        		apaAction = (AbstractPanelAction)actNewAction;
        		apaAction.addComponent(mniTemp);
        	}
        }
    }

    public JMenuItem[] addMenuEntriesToActionsMenu(List lstMenuEntries) {

        JMenuItem[] aryMenuItems            = null;
        ArrayList arylMenuItems             = new ArrayList();
        AbstractPanelAction apaAction       = null;
        int numActions                      = lstMenuEntries.size();

        // remove the old contents from the Actions menu and disable it
        emptyTheActionsMenu();

        if (numActions > 0) {
            // if we have new items for Actions, enable it and load it
            actionsMenu.setEnabled(true);

            JMenuItem mniTemp           = null;
            MenuEntry meEntry           = null;
            Iterator itEntries = lstMenuEntries.iterator();

            while (itEntries.hasNext()) {

                meEntry = (MenuEntry)itEntries.next();

                if (meEntry.getID().equals(MenuEntry.ACTION_MENUITEM)) {
                    mniTemp = actionsMenu.add(meEntry.getAction());
                    arylMenuItems.add(mniTemp);

                    if (meEntry.getAction() instanceof AbstractPanelAction) {
                        apaAction = (AbstractPanelAction)meEntry.getAction();
                        apaAction.addComponent(mniTemp);
                    }
                } else if (meEntry.getType() == MenuEntry.SEPARATOR) {
                    actionsMenu.addSeparator();
                }
            }

            aryMenuItems = new JMenuItem[arylMenuItems.size()];
            arylMenuItems.toArray(aryMenuItems);
        } else {
            aryMenuItems = new JMenuItem[] {};
        }
        return aryMenuItems;
    }

    public JMenuItem[] setActions(Action[] actions) {
        emptyTheActionsMenu();
        return addActions(actions);
    }

    public JMenuItem[] setActions(java.util.List /*<Action>*/ actions) {
        emptyTheActionsMenu();
        return addActions(actions);
    }

    public void removeActionItem(JMenuItem item) {
        actionsMenu.remove(item);
        if (actionsMenu.getItemCount() == 0) {
            actionsMenu.setEnabled(false);
        }
    }

    public void removeActionItem(String itemLabel) {
        int i = 0;
        boolean removed = false;
        int itemCount = actionsMenu.getItemCount();
        while ((!removed) && (i < itemCount)) {
            JMenuItem mi = actionsMenu.getItem(i);
//            String label = mi.getText();
            if (mi.getText().equals(itemLabel)) {
                actionsMenu.remove(i);
                removed = true;
            } else {
                i++;
            }
        }
    }
}//end ConsoleMenuBar




class ExitAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public ExitAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.EXIT_HEADER);
        this.putValue(SHORT_DESCRIPTION, "Terminates program execution."); //$NON-NLS-1$
        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
        if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().exitItemSelected();
            }
        }
    }
}//end ExitAction




class URLsAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public URLsAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.URLS_HEADER);
        this.putValue(SHORT_DESCRIPTION,
        		"Allows user to update default list of URLs for logon."); //$NON-NLS-1$
        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
        if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().urlsItemSelected();
            }
        }
    }
}//end URLsAction




class AddConnectionAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public AddConnectionAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.ADD_CONNECTION_HEADER);
        this.putValue(SHORT_DESCRIPTION,
        		"Allows user to add another server connection."); //$NON-NLS-1$
        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
        if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().addConnectionItemSelected();
            }
        }
    }
}//end AddConnectionAction




class RemoveConnectionAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public RemoveConnectionAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.REMOVE_CONNECTION_HEADER);
        this.putValue(SHORT_DESCRIPTION,
        		"Allows user to close an existing server connection."); //$NON-NLS-1$
        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
        if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().removeConnectionItemSelected();
            }
        }
    }
}//end RemoveConnectionAction




class LoggingPreferencesAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public LoggingPreferencesAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.LOGGING_HEADER);
        this.putValue(SHORT_DESCRIPTION, 
        		"Allows user to specify log message filtering."); //$NON-NLS-1$
        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
        if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().loggingItemSelected();
            }
        }
    }
}//end LoggingAction




class RefreshRatesAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public RefreshRatesAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.REFRESH_RATES_HEADER);
        this.putValue(SHORT_DESCRIPTION, "Allows the user to select intervals " //$NON-NLS-1$
        		+ "for panels that can be auto-refreshed."); //$NON-NLS-1$
        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
        if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().refreshRatesItemSelected();
            }
        }
    }
}//end RefreshRatesAction




class RefreshAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public RefreshAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.REFRESH_HEADER);
        this.putValue(Action.SMALL_ICON, 
        		DeployPkgUtils.getIcon("icon.refresh")); //$NON-NLS-1$
        this.putValue(SHORT_DESCRIPTION, 
        		"Refreshes all data on displayed panel."); //$NON-NLS-1$
        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
		if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().refreshItemSelected();
            }
        }
    }
}//end RefreshAction




class PrintAction extends AbstractPanelAction {
//    private ConsoleMenuBar menuBar;

    public PrintAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.PRINT_HEADER);
//        menuBar = mb;
    }

    public void actionImpl(ActionEvent ev) {
		// There is NO default behavior for print
    }
}//end PrintAction




class AboutAction extends AbstractPanelAction {
    private ConsoleMenuBar menuBar;

    public AboutAction(ConsoleMenuBar mb) {
        super(ConsoleMenuBar.ABOUT_HEADER);
        menuBar = mb;
        this.putValue(SHORT_DESCRIPTION,
        		"Displays Console release information."); //$NON-NLS-1$
    }

    public void actionImpl(ActionEvent ev) {
        if (menuBar != null) {
            if (menuBar.getTheListener() != null) {
                menuBar.getTheListener().aboutItemSelected();
            }
        }
    }
}//end AboutAction
