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
package com.metamatrix.console.ui.views.syslog;

import java.awt.Component;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.DefaultListModel;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ListDataEvent;
import javax.swing.event.ListDataListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.log.reader.LogEntry;
import com.metamatrix.common.log.reader.LogEntryPropertyNames;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.ServerLogManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.util.ColumnSortInfo;
import com.metamatrix.console.ui.util.TableCellRendererFactory;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.PropertyProvider;
import com.metamatrix.console.ui.util.property.TypeConstants;
import com.metamatrix.console.util.AutoRefreshable;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.ZipFileUtil;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.AccumulatorPanel;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellRenderer;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

/**
 *
 * @author  dflorian
 * @version
 */
public final class SysLogPanel
    extends JPanel
    implements  TimeSpanPanelValidityListener,
                ActionListener,
                ListDataListener,
                ListSelectionListener,
                TypeConstants,
                WorkspacePanel,
                MaxRecordsPerQueryListener,
                AutoRefreshable {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

	public final static String LAST_SAVE_DIR_PREF_NAME = 
			"metamatrix.console.lastsyslogsavedirectory"; //$NON-NLS-1$
    public final static String MAX_LOG_ROWS_RETURNED = "metamatrix.log.maxRows"; //$NON-NLS-1$
    
	public final static int WARNING_THRESHOLD_VALUE = 20000;
	public static boolean SUPPRESS_HIGH_LIMIT_WARNING = false;
	public static int MAX_ROWS_VALUE = -1;
	public static boolean MAX_ROWS_HAS_BEEN_RESET = false;						
			


    private static final String DBNAME_SUFFIX = ".dbname"; //$NON-NLS-1$
    private static final String HEADER_SUFFIX = ".hdr"; //$NON-NLS-1$
    private static final String MSG_LEVEL_PREFIX = "msglevel."; //$NON-NLS-1$
    private static final String POSITION_SUFFIX = ".pos"; //$NON-NLS-1$

    private static final String DB_TIMESTAMP_NAME =
        getString("timestamp" + DBNAME_SUFFIX); //$NON-NLS-1$
    private static final String DB_LEVEL_NAME =
        getString("level" + DBNAME_SUFFIX); //$NON-NLS-1$
    private static final String DB_CONTEXT_NAME =
        getString("context" + DBNAME_SUFFIX); //$NON-NLS-1$
    private static final String DB_HOST_NAME =
        getString("host" + DBNAME_SUFFIX); //$NON-NLS-1$
    private static final String DB_PROCESS_NAME =
        getString("process" + DBNAME_SUFFIX); //$NON-NLS-1$
    private static final String DB_THREAD_NAME =
        getString("thread" + DBNAME_SUFFIX); //$NON-NLS-1$
    private static final String DB_MSG_NAME =
        getString("msg" + DBNAME_SUFFIX); //$NON-NLS-1$
    private static final String DB_EXCEPTION_NAME =
        getString("exception" + DBNAME_SUFFIX); //$NON-NLS-1$

    private static final int LEVEL_COL =
        SysLogUtils.getInt(DB_LEVEL_NAME + POSITION_SUFFIX, 4);
    private static final int EXCEPTION_COL =
        SysLogUtils.getInt(DB_EXCEPTION_NAME + POSITION_SUFFIX, 0);
    private static final int TIME_COL =
        SysLogUtils.getInt(DB_TIMESTAMP_NAME + POSITION_SUFFIX, 1);
    private static final int MSG_COL =
        SysLogUtils.getInt(DB_MSG_NAME + POSITION_SUFFIX, 2);

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private LabelWidget lblRecNum;
    private ConsoleAccumulatorPanel pnlAccum;
    private TimeSpanPanel pnlTimeSpan;
    private MaxRecordsPerQueryPanel pnlMaxRows;
    private JPanel pnlContexts;
    private JPanel pnlFilter;
    private JPanel pnlEntries;
    private Splitter splitMain;
    private Splitter splitMinor;
    private TableWidget tblEntries;
    private JTextArea txaMessage;
    private JTextArea txaException;
    private TextFieldWidget txfDetailTime;
    private TextFieldWidget txfLevel;
    private TextFieldWidget txfContext;
    private TextFieldWidget txfHost;
    private TextFieldWidget txfProcess;
    private TextFieldWidget txfThread;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private ArrayList actions = new ArrayList();
    private PanelAction actionApply;
    private PanelAction actionReset;
    private PanelAction actionWriteFile;
    private PanelAction actionExportLogs;
    private String[] dbColumnNames;
    private DefaultTableModel tblModel;
    private String[] headers;
    private Vector headersAsVector;
    private Vector levelCheckBoxes;
    private int rowCount = 0;
    private boolean[] originalLevels;
    private boolean levelSelected;
    private boolean levelsDiffFromOriginal;
    private boolean contextsDiffFromOriginal;
    private boolean timeSpanValid = true;
    private boolean contextSelected = true;
    private boolean timeSpanChangedFromOriginal = false;
    private SimpleDateFormat formatter;
    private AutoRefresher arRefresher = null;
	private int paintCount = 0;
	private ConnectionInfo connection;
	private ServerLogManager logManager;
	private int startingMaxRows;
	private boolean maxRowsDiffFromOriginal = false;
	
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public SysLogPanel(ConnectionInfo conn) throws Exception {
    	super();
    	this.connection = conn;
    	logManager = ModelManager.getServerLogManager(connection);
        setLayout(new GridLayout(1, 1, 10, 10));
        setBorder(new EmptyBorder(new Insets(5, 5, 5, 5)));
        
        
        construct();

        // default to all contexts selected
        java.util.List allContexts = new ArrayList(logManager.getAllContexts());        
        pnlAccum = new ConsoleAccumulatorPanel(allContexts);
        pnlAccum.setInitialValues(allContexts);
        pnlAccum.setValues(allContexts);
        pnlAccum.setAllowsReorderingValues(false);
      	pnlAccum.setMinimumValuesAllowed(0);
        pnlAccum.getAcceptButton().setVisible(false);
        pnlAccum.getCancelButton().setVisible(false);
        pnlAccum.remove(pnlAccum.getNavigationBar());
        pnlAccum.getAvailableValuesHeader().setText(
                getString("pnlAccum.availablevalues.hdr")); //$NON-NLS-1$
        pnlAccum.getValuesHeader().setText(
                getString("pnlAccum.selectedvalues.hdr")); //$NON-NLS-1$
        pnlAccum.addListDataListener(this);
        pnlContexts.add(pnlAccum);

        java.util.List columnNames = LogEntryPropertyNames.COLUMN_NAMES;
		int size = columnNames.size();
        dbColumnNames = new String[size];
        headers = new String[size];
        headersAsVector = new Vector(size);
        headersAsVector.setSize(size);
        for (int i=0; i<size; i++) {
            String dbName = (String)columnNames.get(i);
            int position = SysLogUtils.getInt(dbName + POSITION_SUFFIX, i);
            String hdr = getString(dbName + HEADER_SUFFIX);
            headers[position] = hdr;
            headersAsVector.setElementAt(hdr, position);
            dbColumnNames[position] = dbName;
        }
        tblModel = SysLogUtils.setup(tblEntries, headers,
                SysLogUtils.getInt("tblrows", 20), null); //$NON-NLS-1$
        PropertyProvider propProvider = PropertyProvider.getDefault();
        String patternKey = "date.formatter.date-time"; //$NON-NLS-1$
        String formatKey = SysLogUtils.getString("formatterKey", true); //$NON-NLS-1$
        if (formatKey != null) {
            patternKey = formatKey;
        }
        formatter = (SimpleDateFormat)propProvider.getObject(patternKey);
        checkResetState();
    }

    /////////////////////////////////////////////////////////////////
    // METHODS
    /////////////////////////////////////////////////////////////////

    public void actionPerformed(ActionEvent theEvent) {
/* an action event is fired when the calendar panel button is
selected to drop down the window and then selected again to
close the window. i'm not sure why. that's why checkResetState()
is called within each block below. need to investigate this later.
*/
        Object source = theEvent.getSource();
        if (source instanceof CheckBox) {
            // msg level
            levelsDiffFromOriginal = false;
            levelSelected = false;
            // the first checkbox is null so start with 1
            // this is done so that the level will equal the index
            // one level must be selected or submit is disabled
            for (int size=levelCheckBoxes.size(), i=1; i<size; i++) {
                CheckBox chk = (CheckBox)levelCheckBoxes.get(i);
                if (!levelSelected && chk.isSelected()) {
                    levelSelected = true;
                }
                if (!levelsDiffFromOriginal &&
                    (chk.isSelected() != originalLevels[i])) {
                    levelsDiffFromOriginal = true;
                }
                if (levelSelected && levelsDiffFromOriginal) {
                    break;
                }
            }
            checkResetState();
        }
    }

    private void checkResetState() {
        actionApply.setEnabled((timeSpanValid && levelSelected && contextSelected));
        actionReset.setEnabled((levelsDiffFromOriginal || timeSpanChangedFromOriginal ||
        		contextsDiffFromOriginal || maxRowsDiffFromOriginal));
    }

    private void clearDetailPanel() {
        txfDetailTime.setText(""); //$NON-NLS-1$
        txfLevel.setText(""); //$NON-NLS-1$
        txfContext.setText(""); //$NON-NLS-1$
        txfHost.setText(""); //$NON-NLS-1$
        txfProcess.setText(""); //$NON-NLS-1$
        txfThread.setText(""); //$NON-NLS-1$
        txaMessage.setText(""); //$NON-NLS-1$
        txaException.setText(""); //$NON-NLS-1$
    }

    private void construct() throws Exception {

        // construct actions to be associated with buttons later
        actionApply = new PanelAction(PanelAction.APPLY);
        actionApply.setEnabled(false);
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionApply));
        actionReset = new PanelAction(PanelAction.RESET);
        actionReset.setEnabled(false);
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionReset));
        actionWriteFile = new PanelAction(PanelAction.WRITE_FILE);
        actionWriteFile.setEnabled(false);
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionWriteFile));
        actionExportLogs = new PanelAction(PanelAction.EXPORT_LOGS);
        actionExportLogs.setEnabled(true);
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionExportLogs));
        
        final Insets INSETS = new Insets(3, 3, 0, 0);

        splitMain = new Splitter(SwingConstants.HORIZONTAL);
        splitMain.setOneTouchExpandable(true);
        add(splitMain);

        //-------------------------------//
        //---------- pnlFilter ----------//
        //-------------------------------//

        pnlFilter = new JPanel(new GridBagLayout());
        splitMain.setTopComponent(new JScrollPane(pnlFilter));

        GridBagConstraints gbcFilter = new GridBagConstraints();
        gbcFilter.gridx = 0;
        gbcFilter.gridy = 0;
        gbcFilter.insets = INSETS;

        JTabbedPane tpnFilter = new JTabbedPane();
        gbcFilter.fill = GridBagConstraints.BOTH;
        gbcFilter.weightx = 1.0;
        gbcFilter.weighty = 1.0;
        pnlFilter.add(tpnFilter, gbcFilter);

        //-------------------------------------//
        //---------- pnlTimeSpan --------------//
        //-------------------------------------//

        Date serverStartTime = null;
        try {
            serverStartTime = getRuntimeAPI().getServerStartTime();
		} catch (Exception ex) {
			//Just throw the exception
			throw ex;
        }
        if (serverStartTime == null) {
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.HOUR, -1);
            serverStartTime = cal.getTime();
        }
        pnlTimeSpan = new TimeSpanPanel(serverStartTime, new Date(), this);
        tpnFilter.addTab(getString("tpnFilter.timeframe.tab"), pnlTimeSpan); //$NON-NLS-1$

        //-------------------------------//
        //---------- pnlLevels ----------//
        //-------------------------------//

        JPanel pnlLevels = new JPanel(new GridBagLayout());
        tpnFilter.addTab(getString("tpnFilter.levels.tab"), pnlLevels); //$NON-NLS-1$

        GridBagConstraints gbcLevels = new GridBagConstraints();
        gbcLevels.gridx = 0;
        gbcLevels.gridy = 0;

        LabelWidget lblLevelMsg = new LabelWidget(getString("lblLevelMsg")); //$NON-NLS-1$
        gbcLevels.gridwidth = GridBagConstraints.REMAINDER;
        gbcLevels.insets = new Insets(20, 3, 20, 3);
        pnlLevels.add(lblLevelMsg, gbcLevels);

        // create a checkbox for each message level
        int maxLevel = MessageLevel.getMaximumLevel();
        int minLevel = MessageLevel.getMinimumLevel();
        levelCheckBoxes = new Vector(maxLevel - minLevel + 1);
        levelCheckBoxes.setSize(maxLevel - minLevel + 1);

        originalLevels = new boolean[maxLevel - minLevel + 1];

        gbcLevels.gridy = 1;
        gbcLevels.insets = new Insets(3, 3, 3, 10);
        gbcLevels.gridwidth = 1;

        // skip over the first level which is no reporting
        // this is done so that the level will stay the same as the index
        for (int i=minLevel+1; i<maxLevel+1; i++) {
            CheckBox chk = new CheckBox(getString(MSG_LEVEL_PREFIX + i));
            chk.addActionListener(this);
            levelCheckBoxes.set(i, chk);
            pnlLevels.add(chk, gbcLevels);
            gbcLevels.gridx++;
        }

        // set default selections
        //defaultLevelIndexes will contain the indexes of levels which are selected by default
        int[] defaultLevelIndexes = (int[])SysLogUtils.getObject("msglevel.defaults"); //$NON-NLS-1$
        if (defaultLevelIndexes != null) {
            if (defaultLevelIndexes.length > 0) {
                levelSelected = true;
            }
            for (int i=0; i<defaultLevelIndexes.length; i++) {
                CheckBox chk = (CheckBox)levelCheckBoxes.get(defaultLevelIndexes[i]);
                chk.setSelected(true);
                originalLevels[defaultLevelIndexes[i]] = true;
            }
        }

        //---------------------------------//
        //---------- pnlContexts ----------//
        //---------------------------------//

        pnlContexts = new JPanel(new GridLayout(1, 1));
        tpnFilter.addTab(getString("tpnFilter.contexts.tab"), pnlContexts); //$NON-NLS-1$

		int warningThresholdVal;
		if (SysLogPanel.SUPPRESS_HIGH_LIMIT_WARNING) {
			warningThresholdVal = -1;
		} else {
			warningThresholdVal = SysLogPanel.WARNING_THRESHOLD_VALUE;
		}
		if (!SysLogPanel.MAX_ROWS_HAS_BEEN_RESET) {
            String maxRowsString = getConfigAPI().getCurrentConfiguration().getProperty(MAX_LOG_ROWS_RETURNED);
			try {
			    SysLogPanel.MAX_ROWS_VALUE = Integer.parseInt(maxRowsString);
            } catch (NumberFormatException e) {                
            }
		}
		startingMaxRows = SysLogPanel.MAX_ROWS_VALUE;
		pnlMaxRows = new MaxRecordsPerQueryPanel(this, warningThresholdVal,
				SysLogPanel.MAX_ROWS_VALUE);
		tpnFilter.addTab(getString("tpnFilter.maxrows.tab"), pnlMaxRows); //$NON-NLS-1$
		
        //----------------------------//
        //---------- pnlOps ----------//
        //----------------------------//

        JPanel pnlOps = new JPanel();
        gbcFilter.gridx = 1;
        gbcFilter.fill = GridBagConstraints.NONE;
        gbcFilter.weightx = 0.0;
        gbcFilter.weighty = 0.0;
        pnlFilter.add(pnlOps, gbcFilter);

        JPanel pnlOpsSizer = new JPanel(new GridLayout(2, 1, 0, 5));
        pnlOps.add(pnlOpsSizer);

        ButtonWidget btnApply = new ButtonWidget();
        actionApply.addComponent(btnApply);
        pnlOpsSizer.add(btnApply);

        ButtonWidget btnReset = new ButtonWidget();
        actionReset.addComponent(btnReset);
        pnlOpsSizer.add(btnReset);

        splitMinor = new Splitter(SwingConstants.HORIZONTAL);
        splitMinor.setOneTouchExpandable(true);
        splitMain.setBottomComponent(splitMinor);

        //--------------------------------//
        //---------- pnlEntries ----------//
        //--------------------------------//

        pnlEntries = new JPanel(new GridLayout(1, 1));
        splitMinor.setTopComponent(pnlEntries);

        tblEntries = new TableWidget();
        tblEntries.setEditable(false);
        tblEntries.getSelectionModel().addListSelectionListener(this);

        JScrollPane spnEntries = new JScrollPane(tblEntries);
        pnlEntries.add(spnEntries);

        //-------------------------------//
        //---------- pnlDetail ----------//
        //-------------------------------//

        JPanel pnlDetail = new JPanel(new GridBagLayout());

        JScrollPane spn = new JScrollPane(pnlDetail);
        splitMinor.setBottomComponent(spn);

        GridBagConstraints gbcDetail = new GridBagConstraints();
        gbcDetail.gridx = 0;
        gbcDetail.gridy = 0;
        gbcDetail.insets = INSETS;

        LabelWidget lblDetailTime = new LabelWidget(getString("lblDetailTime")); //$NON-NLS-1$
        gbcDetail.anchor = GridBagConstraints.EAST;
        pnlDetail.add(lblDetailTime, gbcDetail);

        txfDetailTime = GuiComponentFactory.createTextField("timestamp"); //$NON-NLS-1$
        txfDetailTime.setEditable(false);
        gbcDetail.gridx = 1;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(txfDetailTime, gbcDetail);

        LabelWidget lblLevel = new LabelWidget(getString("lblLevel")); //$NON-NLS-1$
        gbcDetail.gridx = 2;
        gbcDetail.anchor = GridBagConstraints.EAST;
        pnlDetail.add(lblLevel, gbcDetail);

        txfLevel = GuiComponentFactory.createTextField("loglevelname"); //$NON-NLS-1$
        txfLevel.setEditable(false);
        gbcDetail.gridx = 3;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(txfLevel, gbcDetail);

        lblRecNum = new LabelWidget();
        gbcDetail.gridx = 4;
        pnlDetail.add(lblRecNum, gbcDetail);

        LabelWidget lblContext = new LabelWidget(getString("lblContext")); //$NON-NLS-1$
        gbcDetail.gridx = 0;
        gbcDetail.gridy = 1;
        gbcDetail.anchor = GridBagConstraints.EAST;
        pnlDetail.add(lblContext, gbcDetail);

        txfContext = GuiComponentFactory.createTextField("logcontextname"); //$NON-NLS-1$
        txfContext.setEditable(false);
        gbcDetail.gridx = 1;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(txfContext, gbcDetail);

        LabelWidget lblThread = new LabelWidget(getString("lblThread")); //$NON-NLS-1$
        gbcDetail.gridx = 2;
        gbcDetail.anchor = GridBagConstraints.EAST;
        pnlDetail.add(lblThread, gbcDetail);

        txfThread = GuiComponentFactory.createTextField("threadname"); //$NON-NLS-1$
        txfThread.setEditable(false);
        gbcDetail.gridx = 3;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(txfThread, gbcDetail);

        LabelWidget lblHost = new LabelWidget(getString("lblHost")); //$NON-NLS-1$
        gbcDetail.gridx = 0;
        gbcDetail.gridy = 2;
        gbcDetail.anchor = GridBagConstraints.EAST;
        gbcDetail.gridwidth = 1;
        pnlDetail.add(lblHost, gbcDetail);

        txfHost = GuiComponentFactory.createTextField(HOST_NAME);
        txfHost.setEditable(false);
        gbcDetail.gridx = 1;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(txfHost, gbcDetail);

        LabelWidget lblProcess = new LabelWidget(getString("lblProcess")); //$NON-NLS-1$
        gbcDetail.gridx = 2;
        gbcDetail.anchor = GridBagConstraints.EAST;
        pnlDetail.add(lblProcess, gbcDetail);

        txfProcess = GuiComponentFactory.createTextField(PROCESS_NAME);
        txfProcess.setEditable(false);
        gbcDetail.gridx = 3;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(txfProcess, gbcDetail);

        LabelWidget lblMessage = new LabelWidget(getString("lblMessage")); //$NON-NLS-1$
        gbcDetail.gridx = 0;
        gbcDetail.gridy = 3;
        gbcDetail.anchor = GridBagConstraints.EAST;
        gbcDetail.gridwidth = 1;
        pnlDetail.add(lblMessage, gbcDetail);

        txaMessage = new JTextArea();
        txaMessage.setWrapStyleWord(true);
        txaMessage.setLineWrap(true);
        txaMessage.setEditable(false);
        txaMessage.setRows(SysLogUtils.getInt("txaMessage.rows", 2)); //$NON-NLS-1$

        JScrollPane spnMessage = new JScrollPane(txaMessage);
        gbcDetail.gridx = 1;
        gbcDetail.gridwidth = GridBagConstraints.REMAINDER;
        gbcDetail.weightx = 1.0;
        gbcDetail.weighty = 1.0;
        gbcDetail.fill = GridBagConstraints.BOTH;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(spnMessage, gbcDetail);

        LabelWidget lblException = new LabelWidget(getString("lblException")); //$NON-NLS-1$
        gbcDetail.gridx = 0;
        gbcDetail.gridy = 4;
        gbcDetail.gridwidth = 1;
        gbcDetail.weightx = 0.0;
        gbcDetail.weighty = 0.0;
        gbcDetail.fill = GridBagConstraints.NONE;
        gbcDetail.anchor = GridBagConstraints.EAST;
        pnlDetail.add(lblException, gbcDetail);

        txaException = new JTextArea();
        txaException.setWrapStyleWord(true);
        txaException.setLineWrap(true);
        txaException.setEditable(false);
        txaException.setRows(SysLogUtils.getInt("txaException.rows", 2)); //$NON-NLS-1$

        JScrollPane spnException = new JScrollPane(txaException);
        gbcDetail.gridx = 1;
        gbcDetail.gridwidth = GridBagConstraints.REMAINDER;
        gbcDetail.weightx = 1.0;
        gbcDetail.weighty = 1.0;
        gbcDetail.fill = GridBagConstraints.BOTH;
        gbcDetail.anchor = GridBagConstraints.WEST;
        pnlDetail.add(spnException, gbcDetail);

        pnlFilter.setPreferredSize(pnlFilter.getMinimumSize());
        pnlDetail.setMinimumSize(pnlDetail.getPreferredSize());

        // Establish AutoRefresher
        arRefresher = new AutoRefresher(this, 15, false, connection);
        arRefresher.init();
        arRefresher.startTimer();
    }

	public void doNotDisplayWarningMessage() {
		SysLogPanel.SUPPRESS_HIGH_LIMIT_WARNING = true;
	}
	
	public void maximumChanged(int newVal) {
		SysLogPanel.MAX_ROWS_VALUE = newVal;
		SysLogPanel.MAX_ROWS_HAS_BEEN_RESET = true;
		maxRowsDiffFromOriginal = (newVal != startingMaxRows);
		checkResetState();
	}
	
    public void timeSpanValidityChanged(boolean isNowValid) {
        timeSpanValid = isNowValid;
        checkResetState();
    }

    public void timeSpanChangedFromOriginal(boolean nowChanged) {
        timeSpanChangedFromOriginal = nowChanged;
        checkResetState();
    }

    public void contentsChanged(ListDataEvent theEvent) {
        // required by the ListDataListener interface
        Collection currVals = pnlAccum.getValues();
        Collection initVals = pnlAccum.getInitialValues();
        contextsDiffFromOriginal = ! (currVals.containsAll(initVals) && (currVals.size() == initVals.size()));
        if (currVals.size() == 0){
            contextSelected = false;
        }else {
            contextSelected = true;
        }
        checkResetState();
    }

    
    /**
     * Get selected levels, as a List of Strings 
     * @return
     * @since 4.3
     */
    private List getSelectedContexts() {
        // If all of the contexts are selected, return null.  SysLogManager methods accept null to mean all contexts.
        java.util.List availableValues = pnlAccum.getAvailableValues();
        if ((availableValues == null) || availableValues.isEmpty()) {
            return null;
        }
        
        return new ArrayList(pnlAccum.getValues());
    }


     
    /**
     * Get selected levels, as a List of Integers 
     * @return
     * @since 4.3
     */
    private List getSelectedLevels() {
        List levels = new ArrayList();
        int size=levelCheckBoxes.size();
        for (int i=1; i<size; i++) {
            CheckBox chk = (CheckBox)levelCheckBoxes.get(i);
            if (chk.isSelected()) {
                levels.add(new Integer(i));
            }
        }

        return levels;
    }

    

    private static String getString(String theKey) {
        return SysLogUtils.getString(theKey);
    }

    public String getTitle() {
        return getString("title"); //$NON-NLS-1$
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    public void intervalAdded(ListDataEvent theEvent) {
        // required by the ListDataListener interface
        contentsChanged(theEvent);
    }

    public void intervalRemoved(ListDataEvent theEvent) {
        // required by the ListDataListener interface
        contentsChanged(theEvent);
    }

    private void populateTable(List entries) {

        Vector rows = new Vector();
        for (Iterator iter = entries.iterator(); iter.hasNext(); ) {
            LogEntry entry = (LogEntry) iter.next();

            Vector row = new Vector(dbColumnNames.length);
            
            row.add(entry.getException());
            row.add(entry.getDate());
            row.add(entry.getMessage());
            row.add(entry.getContext());
            row.add(new Integer(entry.getLevel()));
            row.add(entry.getHostName());
            row.add(entry.getProcessName());
            row.add(entry.getThreadName());
            
            rows.add(row);
        }
        ColumnSortInfo[] columnSortInfo = ColumnSortInfo.getTableColumnSortInfo(tblEntries);       
        tblModel.setDataVector(rows, headersAsVector);
        ColumnSortInfo.setColumnSortOrder(columnSortInfo, tblEntries);       
        tblEntries.createModelRowMap();        
        tblEntries.setEditable(false);
        SysLogTableCellRenderer rend = new SysLogTableCellRenderer();
		TableColumn col = tblEntries.getColumnModel().getColumn(LEVEL_COL);
        col.setCellRenderer(rend);
		col = tblEntries.getColumnModel().getColumn(EXCEPTION_COL);
        col.setCellRenderer(rend);
		
        col = tblEntries.getColumnModel().getColumn(TIME_COL);
        col.setCellRenderer(
            TableCellRendererFactory.createDateRenderer(formatter));
        col = tblEntries.getColumnModel().getColumn(MSG_COL);
        col.setMaxWidth(SysLogUtils.getInt(DB_MSG_NAME + ".col.width.max", //$NON-NLS-1$
                                           col.getMaxWidth()));
        tblEntries.sizeColumnsToFitData(
            SysLogUtils.getInt("sizedatarowcount", 100)); //$NON-NLS-1$
        rowCount = tblEntries.getRowCount();
        if (rowCount > 0) {
            tblEntries.setRowSelectionInterval(0, 0);
            tblEntries.scrollRectToVisible(tblEntries.getCellRect(0, 0, true));
            tblEntries.requestFocus();
        }
    }

    private void reset() {
        if (levelsDiffFromOriginal) {
            for (int size=levelCheckBoxes.size(), i=1; i<size; i++) {
                CheckBox chk = (CheckBox)levelCheckBoxes.get(i);
                chk.setSelected(originalLevels[i]);
            }
            levelsDiffFromOriginal = false;
        }
        pnlAccum.resetValues();
        pnlTimeSpan.reset();
        pnlMaxRows.setValue(startingMaxRows);
        checkResetState();
    }

    public java.util.List resume() {
        Date systemStartTime = null;
        try {
            systemStartTime = getRuntimeAPI().getServerStartTime();
        } catch (Exception ex) {
        }
        if (systemStartTime != null) {
            pnlTimeSpan.resetStartTime(systemStartTime);
        }
        return actions;
    }

    /**
     * @param showWarnings If true, display a warning when no rows or too many rows are returned.
     */
    private void submitRequest(boolean showWarnings) {
    	int maxRows = pnlMaxRows.getMaxRows();
    	
        // clear table and detail panel first
        tblModel.setNumRows(0);
        clearDetailPanel();

        List levels = getSelectedLevels();
        List contexts = getSelectedContexts();

        try {
            List entries = logManager.getLogEntries(pnlTimeSpan.getStartingTime(),
                                                 pnlTimeSpan.endsNow() ? null : pnlTimeSpan.getEndingTime(),
                                                                 levels,
                                                                 contexts,
                                                                 maxRows);
            
            int size = entries.size();
            if (size != 0) {
                populateTable(entries);
                if (!actionWriteFile.isEnabled()) {
                    actionWriteFile.setEnabled(true);
                }
                    
            } else {
                // no records were returned
                if (actionWriteFile.isEnabled()) {
                    actionWriteFile.setEnabled(false);
                }
                lblRecNum.setText(
                    SysLogUtils.getString("lblRecNum", //$NON-NLS-1$
                                          new Object[] {"0", "0"})); //$NON-NLS-1$ //$NON-NLS-2$
                if (!RowsNotSelectedPanel.isCurrentlyDisplayed()) {
                    RowsNotSelectedPanel pnl = new RowsNotSelectedPanel(size,
                    		maxRows);
                    String title = getString("title.norowsselected"); //$NON-NLS-1$
                    RowsNotSelectedPanel.incrementDisplayedPanelCount();
                    if (showWarnings) {
                        DialogWindow.show(ConsoleMainFrame.getInstance(), title, pnl);
                    }
                }
            }

            tblEntries.setPreferredSize(null);
            tblEntries.revalidate();

            checkResetState();
        } catch (Exception theException) {
            ExceptionUtility.showMessage(theException.getMessage(),
                                         theException);
            LogManager.logError(LogContexts.SYSTEMLOGGING,
                                theException, theException.getMessage()); //$NON-NLS-1$
        }
    }

    public void valueChanged(ListSelectionEvent theEvent) {
        if (!theEvent.getValueIsAdjusting()) {
            clearDetailPanel();
            int row = tblEntries.getSelectedRow();
            if (row != -1) {
                row = tblEntries.convertRowIndexToModel(tblEntries.getSelectedRow());
                for (int i=0; i<dbColumnNames.length; i++) {
                    Object obj = tblModel.getValueAt(row, i);
                    if (obj != null) {
                        String value = obj.toString();
                        if (dbColumnNames[i].equals(DB_TIMESTAMP_NAME)) {
                            txfDetailTime.setText(formatter.format((Date)obj));
                        } else if (dbColumnNames[i].equals(DB_LEVEL_NAME)) {
                            txfLevel.setText(getString(MSG_LEVEL_PREFIX + value));
                        } else if (dbColumnNames[i].equals(DB_CONTEXT_NAME)) {
                            txfContext.setText(value);
                        } else if (dbColumnNames[i].equals(DB_HOST_NAME)) {
                            txfHost.setText(value);
                        } else if (dbColumnNames[i].equals(DB_PROCESS_NAME)) {
                            txfProcess.setText(value);
                        } else if (dbColumnNames[i].equals(DB_THREAD_NAME)) {
                            txfThread.setText(value);
                        } else if (dbColumnNames[i].equals(DB_MSG_NAME)) {
                            txaMessage.setText(value);
                            txaMessage.setCaretPosition(0);
                        } else if (dbColumnNames[i].equals(DB_EXCEPTION_NAME)) {
                        	if (value.trim().equalsIgnoreCase("null")) { //$NON-NLS-1$
                        		value = ""; //$NON-NLS-1$
                        	}
                            txaException.setText(value);
                            txaException.setCaretPosition(0);
                        }
                    }
                }
            }
            lblRecNum.setText(
                SysLogUtils.getString("lblRecNum", //$NON-NLS-1$
                                      new Object[] {""+(row+1), ""+rowCount})); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    // Methods required by the Refreshable interface
    /**
     * Name must uniquely identify this Refreshable object.  Useful to support
     * applying mods to the rate and enabled state by outside agencies.
     */
    public String  getName() {
        return StaticProperties.DATA_SYSLOG;
    }

    /**
     * Used by the AutoRefresher to refresh the display.
     *
     */
    public void refresh() {
        if (timeSpanValid && levelSelected && contextSelected) {   
			//invoke in the Swing Thread, because submitRequest modifies Swing components
            SwingUtilities.invokeLater(
                new Runnable() {
                    public void run() {
                        submitRequest(false);
                    }
                }
            );			
        }
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

    private void writeFile() {
    	//Set default extension of ".txt".
    	String[] extensions = new String[] {"txt"}; //$NON-NLS-1$
    	FileSystemView view = new FileSystemView();
    	String dirTxt = (String)UserPreferences.getInstance().getValue(
    			LAST_SAVE_DIR_PREF_NAME);
		//Start file chooser at same directory saved to last time, if any    			
    	if (dirTxt != null) {
    		try {
    			view.setHome(view.lookup(dirTxt));
        	} catch (Exception ex) {
        		//Any exception that may occur on setting the initial view is
            	//inconsequential.  This is merely a convenience to the user.
        	}
    	}
        FileSystemFilter[] filters = null;
        FileSystemFilter filter = new FileSystemFilter(view, extensions, 
        		"Log Files (*.txt)"); //$NON-NLS-1$
        filters = new FileSystemFilter[] {filter};
        DirectoryChooserPanel pnlChooser = new DirectoryChooserPanel(view, 
        		DirectoryChooserPanel.TYPE_SAVE, filters);
        pnlChooser.setAcceptButtonLabel(getString("chooser.save")); //$NON-NLS-1$
        pnlChooser.setShowPassThruFilter(false);

        DialogWindow.show(this, getString("chooser.title"), pnlChooser); //$NON-NLS-1$
        if (pnlChooser.getSelectedButton() == pnlChooser.getAcceptButton()) {
            DirectoryEntry result =
                (DirectoryEntry)pnlChooser.getSelectedTreeNode();
            // the chooser panel handles if the file can be written to
            // or if it can be created
            String LINE_SEP = System.getProperty("line.separator"); //$NON-NLS-1$
            int cols = tblEntries.getColumnModel().getColumnCount();
            StringBuffer txt = new StringBuffer();

            // append column headers
            TableColumnModel colModel = tblEntries.getColumnModel();
            for (int i=0; i<cols; i++) {
                TableColumn col = colModel.getColumn(i);
                if (i == EXCEPTION_COL) {
                    txt.append(getString("exception.hdr")); //$NON-NLS-1$
                } else {
                    txt.append(col.getHeaderValue());
                }
                if (i < cols-1) {
                    txt.append("\t"); //$NON-NLS-1$
                }
            }
            txt.append(LINE_SEP);

            // append data
            for (int rows=tblModel.getRowCount(), i=0; i<rows; i++) {
                for (int j=0; j<cols; j++) {
                    Object value = tblModel.getValueAt(i, j);
                    if (value != null) {
                        if ((j == LEVEL_COL) || (j == TIME_COL)) {
                            TableCellRenderer rend =
                                tblEntries.getCellRenderer(i,j);
                            JLabel lbl =
                                (JLabel)rend.getTableCellRendererComponent(
                                    tblEntries, value, false, false, i, j);
                            txt.append(lbl.getText());
                        } else {
                            //Replace any tabs for proper formatting in the output .txt file                             
                            txt.append(((String)value).replace('\t',' '));
                        }
                    }
                    if (j < cols-1) {
                        txt.append("\t"); //$NON-NLS-1$
                    }
                }
                txt.append(LINE_SEP);
            }

            // write the file
            try {
            	String filename = result.getNamespace();
				FileWriter writer = new FileWriter(filename);
                writer.write(txt.toString());
                writer.flush();
                writer.close();
            	int index = filename.lastIndexOf(File.separatorChar);
            	String path = filename.substring(0, index);
            	UserPreferences.getInstance().setValue(LAST_SAVE_DIR_PREF_NAME, 
            			path);
            	UserPreferences.getInstance().saveChanges();
            } catch (Exception theException) {
                theException.printStackTrace();
                ExceptionUtility.showMessage(theException.getMessage(),
                                             theException);
                LogManager.logError(LogContexts.SYSTEMLOGGING,
                                    theException,
                                    getClass() + ":writeFile"); //$NON-NLS-1$
            }
        }
    }

    private void exportLogs() {
        //Set default extension of ".txt".
        String[] extensions = new String[] {"zip"}; //$NON-NLS-1$
        FileSystemView view = new FileSystemView();
        String dirTxt = (String)UserPreferences.getInstance().getValue(LAST_SAVE_DIR_PREF_NAME);
        //Start file chooser at same directory saved to last time, if any               
        if (dirTxt != null) {
            try {
                view.setHome(view.lookup(dirTxt));
            } catch (Exception ex) {
                //ignore
            }
        }
        FileSystemFilter[] filters = null;
        FileSystemFilter filter = new FileSystemFilter(view, extensions, "Zip Files (*.zip)"); //$NON-NLS-1$
        filters = new FileSystemFilter[] {filter};
        DirectoryChooserPanel pnlChooser = new DirectoryChooserPanel(view, DirectoryChooserPanel.TYPE_SAVE, filters);
        pnlChooser.setAcceptButtonLabel(getString("chooser.save")); //$NON-NLS-1$
        pnlChooser.setShowPassThruFilter(false);

        DialogWindow.show(this, getString("chooser.export.title"), pnlChooser); //$NON-NLS-1$
        if (pnlChooser.getSelectedButton() == pnlChooser.getAcceptButton()) {
            DirectoryEntry result = (DirectoryEntry)pnlChooser.getSelectedTreeNode();
            String filename = result.getNamespace();
            
            ServerAdmin admin = null;
            try {
                //write the server logs to the zipfile
                admin = getConnection().getServerAdmin();
                byte[] logBytes = admin.exportLogs();
                FileUtils.write(logBytes, new File(filename));

                
                //write the console logs to the zipfile
                String consoleLogDir = StaticProperties.getLogDirectory().getAbsolutePath();
                ZipFileUtil.addAll(new File(filename), consoleLogDir, "console"); //$NON-NLS-1$
            } catch (Exception theException) {
                theException.printStackTrace();
                ExceptionUtility.showMessage(theException.getMessage(), theException);
                LogManager.logError(LogContexts.SYSTEMLOGGING, theException, getClass() + ":exportLogs"); //$NON-NLS-1$
            } 
        }
    }
    
    
	public void paint(Graphics g) {
		super.paint(g);
		if (paintCount == 0) {
			paintCount = 1;
			Double mainLoc = (Double)SysLogUtils.getObject("splitter.main.pos"); //$NON-NLS-1$
			double mainDividerLoc;
			if (mainLoc != null) {
				mainDividerLoc = mainLoc.doubleValue();
			} else {
				mainDividerLoc = 0.30;
			}
			splitMain.setDividerLocation(mainDividerLoc);
		} else if (paintCount == 1) {
			paintCount = 2;
			Double minorLoc = (Double)SysLogUtils.getObject("splitter.minor.pos"); //$NON-NLS-1$
			double minorDividerLoc;
			if (minorLoc != null) {
				minorDividerLoc = minorLoc.doubleValue();
			} else {
				minorDividerLoc = 0.50;
			}
			splitMinor.setDividerLocation(minorDividerLoc);
		}
	}
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
	
    ///////////////////////////////////////////////////////////////////////////
    // PanelAction INNER CLASS
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int APPLY = 0;
        public static final int RESET = 1;
        public static final int WRITE_FILE = 3;
        public static final int EXPORT_LOGS = 4;

        public PanelAction(int theType) {
            super(theType);
            if (theType == APPLY) {
                putValue(NAME, getString("actionApply")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("actionApply.tip")); //$NON-NLS-1$
                setMnemonic(SysLogUtils.getMnemonic("actionApply.mnemonic")); //$NON-NLS-1$
            } else if (theType == RESET) {
                putValue(NAME, getString("actionReset")); //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION, getString("actionReset.tip")); //$NON-NLS-1$
                setMnemonic(SysLogUtils.getMnemonic("actionReset.mnemonic")); //$NON-NLS-1$
            } else if (theType == WRITE_FILE) {
                putValue(SHORT_DESCRIPTION, getString("actionWriteFile.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME, getString("actionWriteFile.menu")); //$NON-NLS-1$
                setMnemonic(SysLogUtils.getMnemonic("actionWriteFile.mnemonic")); //$NON-NLS-1$
            } else if (theType == EXPORT_LOGS) {
                putValue(SHORT_DESCRIPTION, getString("actionExportLogs.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME, getString("actionExportLogs.menu")); //$NON-NLS-1$
                setMnemonic(SysLogUtils.getMnemonic("actionExportLogs.mnemonic")); //$NON-NLS-1$
            } else {
                throw new IllegalArgumentException(
                    SysLogUtils.getString("invalidactiontype", //$NON-NLS-1$
                                          new Object[] {""+theType})); //$NON-NLS-1$
            }
        }
        protected void actionImpl(ActionEvent theEvent) {
            if (type == APPLY) {
                submitRequest(true);
            } else if (type == RESET) {
                reset();
            } else if (type == WRITE_FILE) {
                writeFile();
            } else if (type == EXPORT_LOGS) {
                exportLogs();
            }
        }
    }
    
    
   
    private RuntimeStateAdminAPI getRuntimeAPI() {
        return ModelManager.getRuntimeStateAPI(getConnection());
    }
    

    ///////////////////////////////////////////////////////////////////////////
    // SysLogTableCellRenderer INNER CLASS
    ///////////////////////////////////////////////////////////////////////////

    private class SysLogTableCellRenderer
        extends DefaultTableCellRenderer {

        public Component getTableCellRendererComponent(
            JTable theTable,
            Object theValue,
            boolean theSelectedFlag,
            boolean theHasFocusFlag,
            int theRow,
            int theColumn) {

            Object value = theValue;
            if (theColumn == LEVEL_COL) {
                try {
                    String txt = theValue.toString();
                    int level = Integer.parseInt(txt);
                    value = getString(MSG_LEVEL_PREFIX + level);
                } catch (NumberFormatException theException) {
                    value = theValue;
                }
            } else if (theColumn == EXCEPTION_COL) {
            	boolean isNull;
            	if (value == null) {
            		isNull = true;
            	} else {
            		String str = value.toString();
            		isNull = str.trim().equalsIgnoreCase("null"); //$NON-NLS-1$
            	}
                value = new Boolean((!isNull));
            }
            Component comp = super.getTableCellRendererComponent(
                       theTable,
                       value,
                       theSelectedFlag,
                       theHasFocusFlag,
                       theRow,
                       theColumn);
			return comp;
        }

    }




    class ConsoleDefaultListModel extends DefaultListModel {
        public void add(int index, Object value) {
            super.add(index, value);
            if (value == null) {
                return;
            }

        }
        public void addElement(Object value){
            super.addElement(value);
            java.util.List ac = Arrays.asList(this.toArray());
            Collections.sort(ac);
            int index = ac.indexOf(value);
            if (index != ac.size()-1){
                super.removeElement(value);
                add(index,value);
            }
        }
    }

    class ConsoleAccumulatorPanel extends AccumulatorPanel {
         public ConsoleAccumulatorPanel(java.util.List l){
            super(l);
            super.getResetButton().setVisible(false);
         }

         protected DefaultListModel createDefaultListModel() {
            ConsoleDefaultListModel cdlm = new ConsoleDefaultListModel();
            return cdlm;
        }
    }
    
    
    private ConfigurationAdminAPI getConfigAPI() {
        return ModelManager.getConfigurationAPI(getConnection());
    }
}
