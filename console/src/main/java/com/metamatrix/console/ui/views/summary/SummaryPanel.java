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

package com.metamatrix.console.ui.views.summary;

import java.awt.BorderLayout;
import java.awt.Cursor;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Date;

import javax.swing.Action;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelChangedEvent;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.PanelsTree;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.IconComponent;
import com.metamatrix.console.util.AutoRefreshable;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.DaysHoursMinutesSeconds;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

public class SummaryPanel extends JPanel
                       implements ActionListener,
                                   WorkspacePanel,
                                   AutoRefreshable {
    private ConnectionInfo connection;
    private ArrayList aryActions = new ArrayList();
    private SummaryInfoProvider summaryInfoProvider;
    private String sysURL;
    private String sysName;
    private int activeSessionCount, systemState;
    private Date lastSessionStart, startTime;
    private SummaryHostInfo[] hostInfo;
    private JPanel hostPanel;
    private JPanel sessionsPanel;
    private JPanel systemStatePanel;
    private TextFieldWidget runningTFW;
    private TextFieldWidget startTimeTFW;
    private LabelWidget runJL;
    private LabelWidget startedJL;
	private TextFieldWidget activeSessionCountTFW;
	private TextFieldWidget lastSessionTFW;
    private JPanel stopLightPanel;
    private TableWidget hostTable;
    private String[] hostTableColumns = {"Host Identifier", "Status"};
    private GridBagLayout sysStateLayout;
    private AutoRefresher arRefresher;

	public static final IconComponent RED_LIGHT =
        	new IconComponent(IconFactory.getIconForImageFile("red.gif"));
    public static final IconComponent YELLOW_LIGHT =
        	new IconComponent(IconFactory.getIconForImageFile("yellow.gif"));
    public static final IconComponent GREEN_LIGHT =
        	new IconComponent(IconFactory.getIconForImageFile("green.gif"));
        	
    private boolean columnsSized = false;
    private boolean paintedSinceResumeCalled = false;

	public SummaryPanel(ConnectionInfo conn) {
        super();
		this.connection = conn;
        summaryInfoProvider =
        		ModelManager.getSummaryManager(connection);
        getURL();
        sysName = getSystemName();
        createComponent();
    }

    public void addActionToList(String sId, Action act) {
        aryActions.add(new MenuEntry(sId, act));
    }

    private String getURL() {
        sysURL = getConnection().getURL();
        return sysURL;
    }
    
    public void createComponent() {
        GridBagLayout pnlOuterLayout = new GridBagLayout();
        setLayout(pnlOuterLayout);
        
        LabelWidget systemJL = new LabelWidget("System Name:");
        TextFieldWidget systemName = new TextFieldWidget(120);
        systemName.setEditable(false);
        
        if (sysName == null) {
            systemName.setText("");
        } else {
            systemName.setText(sysName);
        }
        JPanel sysPanel = new JPanel();
        GridBagLayout uls = new GridBagLayout();
        sysPanel.setLayout(uls);
        sysPanel.add(systemJL);
        sysPanel.add(systemName);
        
        uls.setConstraints(systemJL, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 2), 0, 0));
        uls.setConstraints(systemName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(0, 2, 0, 0), 0, 0));
        

        LabelWidget urlJL = new LabelWidget("System URL:");
        TextFieldWidget urlName = new TextFieldWidget(120);
        urlName.setEditable(false);
//        urlName.setName("SummaryPanel.url");
        if (sysURL == null) {
            urlName.setText("");
        } else {
            urlName.setText(sysURL);
        }
        JPanel urlPanel = new JPanel();
        GridBagLayout ul = new GridBagLayout();
        urlPanel.setLayout(ul);
		urlPanel.add(urlJL);
        urlPanel.add(urlName);
        
        ul.setConstraints(urlJL, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 2), 0, 0));
        ul.setConstraints(urlName, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(0, 2, 0, 0), 0, 0));
                
        
        LabelWidget userJL = new LabelWidget("User:");
        TextFieldWidget userName = new TextFieldWidget(120);
        userName.setEditable(false);
        String userText = "";
        try {
        	MetaMatrixPrincipalName princ = 
        			UserCapabilities.getLoggedInUser(connection);
        	userText = princ.getName();
        } catch (Exception ex) {
        	//Will ignore this and leave field blank
        }
        userName.setText(userText);
        JPanel userPanel = new JPanel();
        GridBagLayout userLayout = new GridBagLayout();
        userPanel.setLayout(userLayout);
        userPanel.add(userJL);
        userPanel.add(userName);
        userLayout.setConstraints(userJL, new GridBagConstraints(0, 0, 1, 1,
        		0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
        		new Insets(0, 0, 0, 2), 0, 0));
        userLayout.setConstraints(userName, new GridBagConstraints(1, 0, 1, 1,
        		1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
        		new Insets(0, 2, 0, 0), 0, 0));
        
        JPanel connectionPanel = new JPanel(new GridLayout(2, 2, 16, 5));
        connectionPanel.add(sysPanel);
        connectionPanel.add(userPanel);
        connectionPanel.add(urlPanel);


		systemStatePanel  = buildSysStatePanel();
        TitledBorder tBorder = new TitledBorder("System State");
        tBorder.setTitleJustification(TitledBorder.LEFT);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
        systemStatePanel.setBorder(tBorder);

        hostPanel = buildHostPanel();
        tBorder = new TitledBorder("Hosts");
        tBorder.setTitleJustification(TitledBorder.LEFT);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
        hostPanel.setBorder(tBorder);

        sessionsPanel = buildSessionPanel();
        tBorder = new TitledBorder("Sessions");
        tBorder.setTitleJustification(TitledBorder.LEFT);
        tBorder.setTitleFont(tBorder.getTitleFont().deriveFont(Font.BOLD));
        sessionsPanel.setBorder(tBorder);

        ButtonWidget refreshButton = new ButtonWidget("Refresh");
        refreshButton.addActionListener(new ActionListener() {
        	public void actionPerformed(ActionEvent ev) {
        		refresh();
        	}
        });

        pnlOuterLayout.setConstraints(connectionPanel, new GridBagConstraints(0, 0, 
        		1, 1, 1, 0, GridBagConstraints.WEST, 
        		GridBagConstraints.HORIZONTAL, new Insets(10, 10, 10, 10), 
        		0, 0));
        JPanel upperBoxPanel = new JPanel();
        upperBoxPanel.setLayout(new GridLayout(1, 2, 10, 0));
        upperBoxPanel.add(systemStatePanel);
        upperBoxPanel.add(sessionsPanel);
        JPanel lowerBoxPanel = new JPanel();
        lowerBoxPanel.setLayout(new GridLayout(1, 2, 10, 0));
        lowerBoxPanel.add(hostPanel);
        pnlOuterLayout.setConstraints(upperBoxPanel, new GridBagConstraints(
                0, 1, 1, 1, 1, 0, GridBagConstraints.CENTER, 
                GridBagConstraints.BOTH, new Insets(10, 10, 10, 10), 0, 0));
        pnlOuterLayout.setConstraints(lowerBoxPanel, new GridBagConstraints(
                0, 2, 1, 1, 1, 1, GridBagConstraints.CENTER, 
                GridBagConstraints.BOTH, new Insets(10, 10, 10, 10), 0, 0));
        pnlOuterLayout.setConstraints(refreshButton, new GridBagConstraints(
                0, 3, 1, 1, 0, 0, GridBagConstraints.CENTER, 
                GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));

        add(connectionPanel);
        add(upperBoxPanel);
        add(lowerBoxPanel);
        add(refreshButton);

        // Establish AutoRefresher
        arRefresher = new AutoRefresher(this, 15, false, connection);
        arRefresher.init();
        arRefresher.startTimer();
        
        //Not necessary to call refresh() here, so commenting out.  This is 
        //because resume() will be called first time panel is displayed, and for 
        //SummaryPanel resume() calls refresh().  BWP 11/06/02
        //refresh();
	}

    private void getHostsInfo() {
        try {
            hostInfo = summaryInfoProvider.getHostInfo();
        } catch (Exception e) {
            LogManager.logError(LogContexts.SUMMARY, e,
                    "Error retrieving summary panel host information.");
            hostInfo = null;
        }
    }


    public JPanel buildHostPanel() {
        Object[][] data = getHostTableData();
        DefaultTableModel tableModel = new DefaultTableModel(data, 
        		hostTableColumns);
        hostTable = new SummaryTableWidget(tableModel, true);
        hostTable.setEditable(false);

        hostTable.setName("System Summary host table");
        JScrollPane scrollPane = new JScrollPane(hostTable) {
            public void setBounds(final int x, final int y, final int width, final int height) {
                super.setBounds(x, y, width, height);
                SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        hostTable.sizeColumnsToFitData();
                    }
                });
            }
        };
        JPanel hostPanel = new JPanel(new BorderLayout());
        hostPanel.add(scrollPane, BorderLayout.CENTER);
        return hostPanel;
	}

    private Object[][] getHostTableData() {
        Object[][] data;
        if ((hostInfo == null) || (hostInfo.length == 0)) {
            data = new Object[1][3];
            data[0][0] = "";
            data[0][1] = "";
        } else {
            data = new Object[hostInfo.length][3];
            for (int i = 0; i < hostInfo.length; i++) {
                data[i][0] = hostInfo[i].getHostName();
                if (hostInfo[i].getHostStatus() == SummaryHostInfo.RUNNING) {
                	data[i][1] = "Running";
                } else if (hostInfo[i].getHostStatus() == 
                		SummaryHostInfo.NOT_RUNNING) {
                	data[i][1] = "Not Running";
                }
            }
        }
        return data;
    }
    
    private String getSystemName() {
        try {
            return  summaryInfoProvider.getSystemName();
        } catch (Exception e) {
            LogManager.logError(LogContexts.SUMMARY, e,
                    "Error retrieving system name.");
            return "";
        }
    }


    private void getSystemStateInfo() {
        try {
            systemState =  summaryInfoProvider.getSystemState();
        } catch (Exception e) {
            LogManager.logError(LogContexts.SUMMARY, e,
                    "Error retrieving current system state.");
        }

        try {
            startTime = summaryInfoProvider.getSystemStartUpTime();
        } catch (Exception e) {
			LogManager.logError(LogContexts.SUMMARY, e,
            		"Error retrieving system start-up time.");
        }
	}

    private JPanel buildSysStatePanel() {
        getSystemStateInfo();

        sysStateLayout = new GridBagLayout();
        JPanel sysPanel = new JPanel();

        sysPanel.setLayout(sysStateLayout);
        LabelWidget currentJL = new LabelWidget("Current Status:");
        stopLightPanel = new JPanel();

        currentJL.setName("SummaryPanel.System State1");
        startedJL = new LabelWidget("Started:");
        startedJL.setName("SummaryPanel.System State2");
        startTimeTFW = new TextFieldWidget(105);
        startTimeTFW.setEditable(false);

        runJL = new LabelWidget("Running:");
        runningTFW = new TextFieldWidget(105);
        runningTFW.setEditable(false);
        runningTFW.setName("SummaryPanel.System State3");

		if (startTime == null) {
            startTimeTFW.setText("");

            //make the labels and text fields invisible if there's no start time 
            //to display
            startTimeTFW.setVisible(false);
            runningTFW.setVisible(false);
            runJL.setVisible(false);
            startedJL.setVisible(false);
		} else {
            startTimeTFW.setText(startTime.toString());
            runningTFW.setText(createRunningDate(startTime));
        }
		sysPanel.add(currentJL);
        sysPanel.add(stopLightPanel);
        sysPanel.add(startedJL);
        sysPanel.add(startTimeTFW);
        sysPanel.add(runJL);
        sysPanel.add(runningTFW);

        sysStateLayout.setConstraints(currentJL, new GridBagConstraints(
                0, 0, 1, 1, 0, 0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 10), 0, 0));

        setStopLightPanelConstraints();

        sysStateLayout.setConstraints(startedJL , new GridBagConstraints(
                0, 1, 1, 1, 0, 0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 10), 0, 0));

        sysStateLayout.setConstraints(startTimeTFW, new GridBagConstraints(
                1, 1, 1, 1, 1, 0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));

        sysStateLayout.setConstraints(runJL , new GridBagConstraints(
                0, 2, 1, 1, 0 , 0,
                GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 10), 0, 0));

        sysStateLayout.setConstraints(runningTFW, new GridBagConstraints(
                1, 2, 1, 1, 1, 0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));

        return sysPanel;
    }

    private void setStopLightPanelConstraints() {
        sysStateLayout.setConstraints(stopLightPanel, new GridBagConstraints(
        		1, 0, 1, 1, 0, 0, GridBagConstraints.WEST, 
        		GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
    }

    private void getSessionInfo() {
		try {
            activeSessionCount = summaryInfoProvider.getActiveSessionCount();
        } catch (Exception e) {
            LogManager.logError(LogContexts.SUMMARY, e,
                    "Error retrieving active session count.");
        }
        
        try {
            lastSessionStart = summaryInfoProvider.getLastSessionStartUp();
        } catch (Exception e) {
            LogManager.logError(LogContexts.SUMMARY, e,
                    "Error retrieving last session start time.");
        }

	}

    private String createRunningDate(Date startupDate) {
        String displayString = null;
        if (startupDate !=null) {
            Date currentDate = new Date();
            long startupLong = startupDate.getTime();
            long currentLong = currentDate.getTime();
            long timeDiffInMilliseconds = currentLong - startupLong;
            int timeDiffInSeconds = 0;
            if (timeDiffInMilliseconds > 0) {
                timeDiffInSeconds = (int)(timeDiffInMilliseconds/1000);
            }
            DaysHoursMinutesSeconds dhms = new DaysHoursMinutesSeconds(
            		timeDiffInSeconds);
            displayString = dhms.toDisplayString(false);
		}
		return displayString;
    }

    private JPanel buildSessionPanel() {
        GridBagLayout sessionLayout = new GridBagLayout();
        JPanel sessionPanel = new JPanel(sessionLayout);

        LabelWidget activeSessionCountJL = new LabelWidget("Active:");
        activeSessionCountJL.setName("SummaryPanel.sessionState");

        activeSessionCountTFW = new TextFieldWidget(105);
        activeSessionCountTFW.setEditable(false);
        activeSessionCountTFW.setText(""+activeSessionCount);
        
        lastSessionTFW = new TextFieldWidget(105);
        lastSessionTFW.setEditable(false);
        if (lastSessionStart == null) {
            lastSessionTFW.setText("");
        } else {
            lastSessionTFW.setText(lastSessionStart.toString());
        }
        
        LabelWidget lastSessionJL = new LabelWidget("Last Logged In:");
        lastSessionJL.setName("SummaryPanel.sessionState1");

        sessionLayout.setConstraints(activeSessionCountJL, new GridBagConstraints(0, 0, 
        		1, 1, 0, 0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));

        sessionLayout.setConstraints(activeSessionCountTFW, 
        		new GridBagConstraints(1, 0, 1, 1, 1 , 0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 10), 0, 0));

        sessionLayout.setConstraints(lastSessionJL, new GridBagConstraints(0, 1, 
        		1, 1, 0, 0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));

        sessionLayout.setConstraints(lastSessionTFW , new GridBagConstraints(
                1, 1, GridBagConstraints.REMAINDER, 1, 1 , 0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 10), 0, 0));

        sessionPanel.add(activeSessionCountJL);
        sessionPanel.add(activeSessionCountTFW);
        sessionPanel.add(lastSessionJL);
        sessionPanel.add(lastSessionTFW);

        return sessionPanel;
	}

    public void refreshData() {
        PanelsTree tree = PanelsTree.getInstance(getConnection());
        tree.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        refreshImpl();
        if (!StaticUtilities.isShowingWaitCursor()) {
            tree.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
    }

    private void refreshImpl() {
        getSessionInfo();
        getSystemStateInfo();
        getHostsInfo();
        JPanel stopLightIcon;
		if (systemState == SummaryInfoProvider.GREEN) {
            stopLightIcon = GREEN_LIGHT;
		} else if (systemState == SummaryInfoProvider.YELLOW) {
            stopLightIcon = YELLOW_LIGHT;
		} else {
            stopLightIcon = RED_LIGHT;
		}
        if ((stopLightPanel != stopLightIcon) || (!paintedSinceResumeCalled)) {
            systemStatePanel.remove(stopLightPanel);
            stopLightPanel = stopLightIcon;
            systemStatePanel.add(stopLightPanel);
            setStopLightPanelConstraints();
			ConsoleMainFrame.getInstance().repaintNeeded();
		}
        activeSessionCountTFW.setText(
        		(new Integer(activeSessionCount)).toString());
        if (lastSessionStart != null) {
            lastSessionTFW.setText(lastSessionStart.toString());
        }
        Object[][] hostData = getHostTableData();
        ((DefaultTableModel)hostTable.getModel()).setNumRows(0);
        ((DefaultTableModel)hostTable.getModel()).setDataVector(hostData,
                hostTableColumns);
        hostTable.setEditable(false);
        updateRunning();
        if (!columnsSized) {
        	updateTableColumnWidths();
        	columnsSized = true;
        	
        	//Tried inserting immediate repopulation of table here to see if 
        	//that would cause repaint of column widths to fit data.  But did
        	//not.  Problem has been submitted as Toolbox defect #6224
        	
        	//((DefaultTableModel)hostTable.getModel()).setNumRows(0);
        	//((DefaultTableModel)hostTable.getModel()).setDataVector(hostData,
            //	    hostTableColumns);
        	//((DefaultTableModel)connectionsTable.getModel()).setNumRows(0);
        	//((DefaultTableModel)connectionsTable.getModel()).setDataVector(
            //	    connectionData, connectionsTableColumns);
        }
    }
    
    public void updateTableColumnWidths() {
        hostTable.sizeColumnsToFitData();
    }
    
    private void updateRunning() {
        if (startTime!=null) {
            startTimeTFW.setText(startTime.toString());
            runningTFW.setText(createRunningDate(startTime));

            startedJL.setVisible(true);
            runJL.setVisible(true);
            startTimeTFW.setVisible(true);
            runningTFW.setVisible(true);
        } else {
            startTimeTFW.setText("");
            runningTFW.setText("");

            startedJL.setVisible(false);
            runJL.setVisible(false);
            startTimeTFW.setVisible(false);
            runningTFW.setVisible(false);
		}
    }

    public java.util.List /*<Action>*/ resume() {
    	paintedSinceResumeCalled = false;
		refreshData();
		ArrayList result= new ArrayList();
		return result;
    }

    public String getTitle() {
        return "Summary";
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    public void modelChanged(ModelChangedEvent e) {
        refreshImpl();
	}

    public void actionPerformed(ActionEvent ev) {
    }

	/**
     * Name must uniquely identify this Refreshable object.  Useful to support
     * applying mods to the rate and enabled state by outside agencies.
     */
    public String  getName() {
        return StaticProperties.DATA_SUMMARY;
    }

    public void refresh() {
		refreshData();
    }

    /**
     * Turns the refresh feature on or off.
     *
     */
    public void setAutoRefreshEnabled(boolean b) {
        arRefresher.setAutoRefreshEnabled(b);
    }

    /**
     * Sets the refresh rate.
     *
     */
    public void setRefreshRate(int iRate) {
        arRefresher.setRefreshRate(iRate);
    }

    /**
     * Set the 'AutoRefresh' agent
     *
     */
    public void setAutoRefresher(AutoRefresher ar) {
        arRefresher = ar;
    }

    /**
     * Get the 'AutoRefresh' agent
     *
     */
    public AutoRefresher getAutoRefresher() {
        return arRefresher;
    }
    
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
    
    public void paint(Graphics g) {
    	super.paint(g);
    	paintedSinceResumeCalled = true;
    }
}//end SummaryPanel



//Included as debugging aid, was printing info in paint method.
class SummaryTableWidget extends TableWidget {
	public SummaryTableWidget(DefaultTableModel model, boolean isSortable) {
		super(model, isSortable);
	}
	
	public SummaryTableWidget(DefaultTableModel model) {
		super(model);
	}
	
	public void paint(Graphics g) {
		super.paint(g);
	}
}//end SummaryTableWidget				
