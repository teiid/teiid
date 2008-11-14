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

package com.metamatrix.console.ui.layout;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.Toolkit;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionAndPanel;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.connections.ConnectionProcessor;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.dialog.RefreshRatesDialog;
import com.metamatrix.console.ui.dialog.ServerURLsDialog;
import com.metamatrix.console.ui.util.RepaintController;
import com.metamatrix.console.ui.views.summary.SummaryPanel;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ui.widget.AboutDialog;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;

/**
 *
 */
public class ConsoleMainFrame extends JFrame implements ConsoleMenuBarListener,
        RepaintController {

    public final static String MAIN_FRAME_TITLE = ConsolePlugin.Util.getString("ConsoleMainFrame.title"); //$NON-NLS-1$
    public final static double SCREEN_HEIGHT_PROPORTION = 0.9;
    public final static double SCREEN_WIDTH_PROPORTION = 0.9;
    public final static double SPLITTER_RELATIVE_SETTING = 0.22;
    public final static Class INITIAL_PANEL_CLASS =
            PanelsTreeModel.SUMMARY_PANEL_CLASS;
    public final static boolean PANELS_TREE_ALWAYS_DISPLAYED = true;

    private static ConsoleMainFrame theFrame;
    private ConsoleMenuBar menuBar;
    private JSplitPane splitPane;
    private PanelsTree tree;
    private JPanel treePanel;
    private Workspace workspace;
    private WorkspaceController controller;
//    private TreeAndControllerCoordinator coordinator;
    private boolean hasBeenPainted = false;
    private CDKCallbackHandler handler;
    private boolean showingTree;
    private JComboBox connectionsComboBox;
    private JPanel connectionsPanel;
    private Map /*<ConnectionInfo to PanelsTreeScrollPane>*/ treeMap = 
    		new HashMap();

    private ConsoleMainFrame(boolean showingTree,
    		ConnectionInfo initialConnection) {
        super(MAIN_FRAME_TITLE);
        this.showingTree = showingTree;
        init(initialConnection);
    }

    public static void createInstance(boolean showingTree,
    		ConnectionInfo initialConnection) {
        if (theFrame == null) {
            theFrame = new ConsoleMainFrame(showingTree, initialConnection);
        } else {
            String msg = "Attempt to create duplicate ConsoleMainFrame."; //$NON-NLS-1$
            LogManager.logError(LogContexts.INITIALIZATION, msg);
            throw new RuntimeException(msg);
        }
    }

    public static ConsoleMainFrame getInstance() {
        return theFrame;
    }

    private void init(ConnectionInfo initialConnection) {
		addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                exitItemSelected();
            }
        });
        handler = new CDKCallbackHandler();
        setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension size = new Dimension((int)(screenSize.width * SCREEN_WIDTH_PROPORTION),
                (int)(screenSize.height * SCREEN_HEIGHT_PROPORTION));
        this.setSize(size);
        setLocation(StaticUtilities.centerFrame(getSize()));
		tree = PanelsTree.createInstance(initialConnection);
		PanelsTreeScrollPane treeSP = new PanelsTreeScrollPane(tree);
		treeMap.put(initialConnection, treeSP);
        treePanel = new JPanel();
        treePanel.setBackground(Color.gray);
        setTreeScrollPane(treeSP);
        JPanel leftPanel = new JPanel();
        GridBagLayout ll = new GridBagLayout();
        leftPanel.setLayout(ll);
        Object[] connections = new Object[] {initialConnection};
        connectionsComboBox = new JComboBox(connections);
		Font font = connectionsComboBox.getFont();
		Font newFont = new Font(font.getName(), font.getStyle(),
				font.getSize() - 1);
		connectionsComboBox.setFont(newFont); 
		connectionsComboBox.setSelectedItem(initialConnection);
        connectionsComboBox.addItemListener(new ItemListener() {
        	public void itemStateChanged(ItemEvent ev) {
        		if (ev.getStateChange() == ItemEvent.SELECTED) {
        			connectionSelectionChanged();
        		}
        	}
        });
		connectionsPanel = new JPanel();
		GridBagLayout cl = new GridBagLayout();
		connectionsPanel.setLayout(cl);
		LabelWidget connectionLabel = new LabelWidget("Connection:"); //$NON-NLS-1$
		connectionsPanel.add(connectionLabel);
		cl.setConstraints(connectionLabel, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
				new Insets(0, 0, 0, 0), 0, 0));
		connectionsPanel.add(connectionsComboBox);
		cl.setConstraints(connectionsComboBox, new GridBagConstraints(
				0, 1, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
				GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
		leftPanel.add(connectionsPanel);
		ll.setConstraints(connectionsPanel, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.CENTER, 
				GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
		leftPanel.add(treePanel);
		ll.setConstraints(treePanel, new GridBagConstraints(0, 1, 1, 1,
				1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
				new Insets(0, 0, 0, 0), 0, 0));
				        
		menuBar = ConsoleMenuBar.getInstance();
        ConsoleMenuBar.setTheListener(this);
        this.setJMenuBar(menuBar);
		Workspace.createInstance(this);
		workspace = Workspace.getInstance();
		WorkspaceController.createInstance(workspace);
		controller = WorkspaceController.getInstance();
        splitPane = new Splitter(JSplitPane.HORIZONTAL_SPLIT, true, leftPanel,
            workspace);
        splitPane.setOneTouchExpandable(true);
        if (showingTree) {
        	getContentPane().add(splitPane);
        } else {
        	getContentPane().add(workspace);
        }
        TreeAndControllerCoordinator.createInstance(tree, controller,
        		initialConnection);
//        coordinator = 
            TreeAndControllerCoordinator.getInstance(initialConnection);

        // expand the tree
        StaticTreeUtilities.expandAll(tree);
        
        setConnectionsPanelVisible(false);
    }
    
    private void setTreeScrollPane(PanelsTreeScrollPane treeSP) {
    	treePanel.removeAll();
        GridBagLayout tl = new GridBagLayout();
        treePanel.setLayout(tl);
        treePanel.add(treeSP);
        tl.setConstraints(treeSP, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 2), 0, 0));
		tree = treeSP.getTree();
    }
    
    public void setConnectionsPanelVisible(boolean flag) {
    	connectionsPanel.setVisible(flag);
    }
    
    public ConnectionInfo[] getConnections() {
    	int numConnections = treeMap.size();
    	ConnectionInfo[] connections = new ConnectionInfo[numConnections];
    	Iterator it = treeMap.keySet().iterator();
    	for (int i = 0; it.hasNext(); i++) {
    		connections[i] = (ConnectionInfo)it.next();
    	}
    	return connections;
    }
    
    public void addConnection(ConnectionInfo connection, PanelsTree tree) {
    	connectionsComboBox.addItem(connection);
    	PanelsTreeScrollPane treeSP = new PanelsTreeScrollPane(tree);
    	treeMap.put(connection, treeSP);
    }
    
    public void removeConnection(ConnectionInfo connection) {
    	connectionsComboBox.removeItem(connection);
    	treeMap.remove(connection);
    }

	private void connectionSelectionChanged() {
		//If not visible, has been called through adding item to selection list.
		//In this case just ignore.
		if (connectionsPanel.isVisible()) {
			ConnectionInfo connection = 
					(ConnectionInfo)connectionsComboBox.getSelectedItem();
			controller.connectionSelectionChanged(connection);
		}
	}
	
	public void selectConnection(ConnectionInfo connection) {
		connectionsComboBox.setSelectedItem(connection);
		this.repaintNeeded();
	}
	
	public void displayTreeForConnection(ConnectionInfo connection) {
		Iterator it = treeMap.entrySet().iterator();
		boolean done = false;
		while (!done) {
			Map.Entry me = (Map.Entry)it.next();
			ConnectionInfo thisConn = (ConnectionInfo)me.getKey();
			if (connection.equals(thisConn)) {
				done = true;
				PanelsTreeScrollPane treeSP = (PanelsTreeScrollPane)me.getValue();
				setTreeScrollPane(treeSP);
			}
		}
	}
	
	public void repaintNeeded() {
    	if (PANELS_TREE_ALWAYS_DISPLAYED) {
	        final int splitterLoc = splitPane.getDividerLocation();
	        int incr = -1;
	        if (splitterLoc == 0) {
	            incr = 1;
	        }
	        final int increment = incr;
	        SwingUtilities.invokeLater(new Runnable() {
	            public void run() {
	                splitPane.setDividerLocation(splitterLoc + increment);
	                splitPane.setDividerLocation(splitterLoc);
	                final WorkspacePanel panel = workspace.getCurrentPanel();
	                if (panel instanceof SummaryPanel) {
	                    final SummaryPanel summary = (SummaryPanel)panel;
	                    summary.addComponentListener(new ComponentAdapter() {
	                        public void componentResized(
	                        		final ComponentEvent event) {
	                            summary.updateTableColumnWidths();
	                            summary.removeComponentListener(this);
	                        }
	                    });
	                }
	            }
        	});
    	} else {
			final int x = this.getLocation().x;
			final int y = this.getLocation().y;
			int incr = -1;
			if (x == 0) {
				incr = 1;
			}
			final int increment = incr;
			final JFrame frame = this;
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					frame.setLocation(new Point(x + increment, y));
					frame.setLocation(new Point(x, y));
				}
			});
    	}
    }

	public void setShowingTree(boolean flag) {
		if (flag != showingTree) {
			showingTree = flag;
			if (showingTree) {
				getContentPane().remove(workspace);
				getContentPane().add(splitPane);
			} else {
				getContentPane().remove(splitPane);
				getContentPane().add(workspace);
			}
			repaintNeeded();
		}
	}
    
    public Component getMainTree() {
        return this.tree;
    }
	
    public void setInitialView(ConnectionInfo connection) {
    	ConnectionAndPanel cp = new ConnectionAndPanel(connection,
    			INITIAL_PANEL_CLASS, null);
        this.selectPanel(cp);
    }

    public void urlsItemSelected() {
        ServerURLsDialog dialog = new ServerURLsDialog(this);
        dialog.createComponent();
        dialog.show();
    }

    public void loggingItemSelected() {
       UserPreferenceCallback callback = new UserPreferenceCallback();
        try {
            handler.handle(callback, this);
        }catch(Exception e) {
            Assertion.failed("There was an unknown error while trying to handle the UserPreferences callback."); //$NON-NLS-1$
        }

    }

    public void refreshItemSelected() {
    	//refresh in a background thread, because this is slow
    	//(especially if the server becomes unavailable)
        Thread thread = new Thread() {
            public void run() {
            	Workspace ws = Workspace.getInstance();
            	if (ws != null) {
            		WorkspacePanel currentPanel = ws.getCurrentPanel();
            		if (currentPanel instanceof Refreshable) {
            			((Refreshable)currentPanel).refresh();
            		}
            	}
            }
        };
        
        thread.start();
    }

    public void refreshRatesItemSelected() {
        RefreshRatesDialog dialog = new RefreshRatesDialog(this);
        dialog.createComponent();
        dialog.show();
    }

    public void aboutItemSelected() {
        String title = ConsolePlugin.Util.getString("ConsoleAboutDialog.title"); //$NON-NLS-1$
        AboutDialog.show(this, title, new ConsoleAboutPanel("", "")); //$NON-NLS-1$ //$NON-NLS-2$
    }

	public void addConnectionItemSelected() {
		ConnectionProcessor cp = ConnectionProcessor.getInstance();
		cp.handleAddConnectionRequest();
	}
	
	public void removeConnectionItemSelected() {
		ConnectionProcessor cp = ConnectionProcessor.getInstance();
		cp.handleRemoveConnectionRequest();
	}
	
    public void selectPanel(ConnectionAndPanel panel) {
        tree.selectNodeForPanel(panel);
    }

    
    /**
     * Hides the console window, then attempts to logoff from the server,
     * then exits the application.
     * @see com.metamatrix.console.ui.layout.ConsoleMenuBarListener#exitItemSelected()
     */
    public void exitItemSelected() {     
        //exit in a background thread, because this is slow
    	//(especially if the server becomes unavailable)
        Thread thread = new Thread() {
            public void run() {
                boolean exiting = true;
                if ((controller != null) && controller.havePendingChanges()) {
                    exiting = controller.finishUp();
                }
            
                if (exiting) {
                    ConsoleMainFrame.this.hide();
                    ConsoleMainFrame.this.logoff();
                    System.exit(0);
                }
            }
        };
            
        thread.start();
    }

    public void exitConsole() {
        exitItemSelected();
    }

    /**
     * Overridden paint() method.  Runs postRealize() method upon first paint.
     */
    public void paint(Graphics g) {
        super.paint(g);
        if (!hasBeenPainted) {
            hasBeenPainted = true;
            SwingUtilities.invokeLater(new Runnable() {
                public void run() {
                    postRealize();
                }
            });
        }
    }

    public void logoff() {
        try {
            StaticProperties.saveProperties();
        } catch (Exception ex) {
    	}
    	ConnectionInfo[] connections = ModelManager.getConnections();
        for (int i = 0; i < connections.length; i++) {
        	try {
        		connections[i].close();
            } catch (Exception e) {
        	}
        }
    }

    private void postRealize() {
        splitPane.setDividerLocation( SPLITTER_RELATIVE_SETTING );
    }

}//end ConsoleMainFrame




class PanelsTreeScrollPane extends JScrollPane {
	private PanelsTree tree;
	
	public PanelsTreeScrollPane(PanelsTree tree) {
		super(tree);
		this.tree = tree;
	}
	
	public PanelsTree getTree() {
		return tree;
	}
}//end PanelsTreeScrollPane
