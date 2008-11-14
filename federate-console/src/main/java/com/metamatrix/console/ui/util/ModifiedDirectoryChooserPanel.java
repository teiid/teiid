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

//################################################################################################################################
package com.metamatrix.console.ui.util;

import java.awt.*;
import java.awt.event.*;
import java.util.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.table.*;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.tree.PassThroughTreeNodeFilter;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeFilter;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.DirectoryEntryEditor;
import com.metamatrix.common.tree.directory.DirectoryEntryFilter;
import com.metamatrix.common.tree.directory.DirectoryEntryView;
import com.metamatrix.common.tree.directory.FileSystemEntry;
import com.metamatrix.common.tree.directory.PassThroughDirectoryEntryFilter;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.util.FileResourceConstants;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.*;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.button.ButtonBorderPainter;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

/**
 * DirectoryChooserPanel is a customizable file explorer dialog similar to JFileChooser.  DirectoryChooserPanel
 * works with DirectoryEntrys and DirectoryEntryViews rather than file systems.
 *
 * There are 2 constructors for DirectoryChooserPanel.  Both constructors need to be passed a TreeView and a type.
 * The 2 types are DirectoryChooserPanel.TYPE_SAVE, used for saving files and DirectoryChooserPanel.TYPE_OPEN used
 * for selecting and opening files.  The second constructor will also take an array of TreeFilters which will populate
 * a filter combo box.
 * @version 2.1
 */
public class ModifiedDirectoryChooserPanel extends DialogPanel {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    public static final int APPROVE_OPTION = JOptionPane.OK_OPTION;
    public static final int CANCEL_OPTION = JOptionPane.CANCEL_OPTION;
    public static final int TYPE_OPEN = 0;
    public static final int TYPE_SAVE = 1;
//    private static final int COL_WIDTH_SIZING_ANALYSIS_ROW_COUNT = 100;

    private int panelType = -1;
//    private int propertyCount;
    private boolean showingTable = true;
    private boolean showNewFolderButton = true;
    private boolean showFilterComboBox = true;
    private boolean showDetailsButton = false;
    private boolean allowFolderCreation = true;
//    private boolean allowNonFolderCreation = true;
//    private boolean showPassThruFilter = true;
    protected boolean multiSelectionAllowed = false;
    private boolean filenameSelectionAllowed = true;
    private JPanel panel;
    protected JPanel bottomDetailsPanel, topButtonsPanel, topDetailsPanel;
    private JComboBox folderComboBox;
    protected JComboBox filterComboBox;
    private TextFieldWidget filenameTextField;
    private DirectoryEntryTable table;
//    private String acceptButtonLabel = "OK"; //$NON-NLS-1$
//    private String cancelButtonLabel = "Cancel"; //$NON-NLS-1$
    protected String INVALID_FILENAME_MSG = "Invalid name. Could not create entry."; //$NON-NLS-1$
    protected String PROTECTED_PARENT_MSG = "Cannot create in this folder."; //$NON-NLS-1$
    private JList list;
    private JScrollPane centralPanel;
    private ButtonWidget upButton, newFolderButton, detailsButton;
    protected DirectoryEntryFilter selectedFilter = null;
    protected PassThroughDirectoryEntryFilter passThruDirectoryEntryFilter;
    private DirectoryEntryFilter[] chooserFilters = null;
    protected DirectoryEntryView directoryEntryView;
    protected DirectoryEntry selectedDirectoryEntry, existingDirectoryEntry, homeDirectoryEntry, parentDirectoryEntry;
    private DirectoryEntryEditor treeNodeEditor;
    private LabelWidget fileTypeLabel = null;
    private Collection rootsList;
    private Collection propertiesToShow = Collections.EMPTY_LIST;
    private Collection rootsToShow = Collections.EMPTY_LIST;
    private Collection selectedDirectoryEntries = Collections.EMPTY_LIST;
    private boolean acceptsFolders;
    private boolean tmpModelerOverride;
    private TableCellRenderer tableCellRenderer;
    private ListCellRenderer listCellRenderer;
    private MouseListener tableMouseListener, listMouseListener;
    private ListSelectionListener tableSelectionListener;
    private MDCPOpenStateListener openStateListener;
    private boolean nameFldUpdating;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Creates a DirectoryChooserPanel with a list of selectable extension filters.
     * @param directoryEntryView the view the DirectoryChooserPanel is going to deal with.
     * @param type the type of DirectoryChooserPanel to create.  Either DirectoryChooserPanel.TYPE_OPEN or DirectoryChooserPanel.TYPE_SAVE
     * @param filters an array of filters that can be added to the panel to filter on entry extensions
     * @param propertiesToShow a collection of PropertyDefinitions which will be displayed in the table view of the
     * DirectoryChooserPanel.  Pass a null if you wish to show all non-hidden properties.
     * @param rootsToShow is a collection of DirectoryEntrys which will be displayed in the fileComboBox if they exist as roots.
     * Pass a null if you wish to show all roots.
     * @since 2.0
     */
    public ModifiedDirectoryChooserPanel(DirectoryEntryView directoryEntryView,
                                int type,
                                DirectoryEntryFilter[] filters,
                                Collection propertiesToShow,
                                Collection rootsToShow,
                                MDCPOpenStateListener openStateListener) {
        //the DirectoryEntryView used through the entire panel
        this.directoryEntryView = directoryEntryView;

        //the collection of PropertyDefinitions to show in the table view
        this.propertiesToShow = propertiesToShow;

        //the collection of roots to show in the folderComboBox
        this.rootsToShow = rootsToShow;

        //a local copy of the system roots list.  Used so the computer system doesn't constantly re-read the drives
        rootsList = getRootsToUse();

        //the home DirectoryEntry where default selections will be set
        homeDirectoryEntry = (DirectoryEntry)directoryEntryView.getHome();

        //number of properties associated with this DirectoryEntryView
//        propertyCount = 
            directoryEntryView.getPropertyDefinitions().size();

        //TreeNodeEditor used for checking read-only status of DirectoryEntries
        treeNodeEditor = directoryEntryView.getDirectoryEntryEditor();

        //the array of TreeNodeFilters
        chooserFilters = filters;

        //the type of panel constructed, SAVE or OPEN
        panelType = type;

		this.openStateListener = openStateListener;
		
        if (panelType == TYPE_OPEN) {
            getAcceptButton().setText("Open"); //$NON-NLS-1$
        } else {
            getAcceptButton().setText("Save"); //$NON-NLS-1$
        }
        setAcceptButtonEnabled(false);

        createComponent();
        setContent(panel);
        
        
        this.addComponentListener(new ComponentAdapter() {
            public void componentResized(final ComponentEvent event) {
                panel.removeComponentListener(this);
                SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        if (filenameTextField.isEnabled()) {
        					filenameTextField.requestFocus();
                        } else {
                            filenameTextField.requestFocus();
                        }
                    }
                });
            }
        });


		folderComboBoxSelectionChanged();
    }
    
    public ModifiedDirectoryChooserPanel(DirectoryEntryView directoryEntryView,
                                int type,
                                DirectoryEntryFilter[] filters,
                                Collection propertiesToShow,
                                Collection rootsToShow) {
		this(directoryEntryView, type, filters, propertiesToShow, rootsToShow,
				null);
	}                                	

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a DirectoryChooserPanel with no selectable extension filters.
    @since 2.0
    */
    public ModifiedDirectoryChooserPanel(DirectoryEntryView directoryEntryView, 
    		int type, MDCPOpenStateListener openStateListener) {
        this(directoryEntryView, type, null, Collections.EMPTY_LIST, 
        		Collections.EMPTY_LIST, openStateListener);
    }

    public ModifiedDirectoryChooserPanel(DirectoryEntryView directoryEntryView, 
    		int type, DirectoryEntryFilter[] filters) {
        this(directoryEntryView, type, filters, Collections.EMPTY_LIST, 
        		Collections.EMPTY_LIST);
    }

    public ModifiedDirectoryChooserPanel(DirectoryEntryView directoryEntryView, 
    		int type, DirectoryEntryFilter[] filters, 
    		MDCPOpenStateListener openStateListener) {
        this(directoryEntryView, type, filters, Collections.EMPTY_LIST, 
        		Collections.EMPTY_LIST, openStateListener);
    }

    public ModifiedDirectoryChooserPanel(DirectoryEntryView directoryEntryView,
                                int type,
                                DirectoryEntryFilter[] filters,
                                Collection propertiesToShow) {
        this(directoryEntryView, type, filters, propertiesToShow, Collections.EMPTY_LIST);
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    /**
     * @since 3.0
     */
    protected void activateAcceptButton() {
        getAcceptButton().doClick();
    }

    /**
     * @since 3.0
     */
    protected void setAcceptButtonEnabled(final boolean enabled) {
		getAcceptButton().setEnabled(enabled);
    }

    /**
	 * @since 3.0
	 */
	protected void updateAcceptButton() {	
		if (panelType == TYPE_OPEN) {
			if (openStateListener != null) {
				checkOpenStateListenerNotifying();
			} else {	
	    	    Collection selections;
	            if (showingTable) {
	                selections = table.getSelectedObjects();
	            } else {
	                selections = Arrays.asList(list.getSelectedValues());
	            }
	            for (final Iterator iter = selections.iterator();  iter.hasNext();) {
	            	
	            	// jh: defect 7964: this collection might contain objects of type
	            	//   DirectoryEntry or ComboData, so check that before you do
	            	//   your cast.
	            	DirectoryEntry deTemp = null;
	            	Object oEntry = iter.next();
	            	if ( oEntry instanceof ComboData ) {
	            		deTemp = ((ComboData)oEntry).getDirectoryEntry();
	            	} else {
	            		deTemp = (DirectoryEntry)oEntry;
	            	}
	            	
	                if (acceptsFolders  ||  deTemp.getType() == FileSystemEntry.TYPE_FILE) {
	                    setAcceptButtonEnabled(true);
	                    return;
	                }
	            }
	            setAcceptButtonEnabled(false);
	            if (showingTable) {
	                setAcceptButtonEnabled(table.getSelectedRowCount() > 0);
	            } else {
	                setAcceptButtonEnabled(!list.isSelectionEmpty());
            	}
            }
	    } else {
            final String name = getNameFieldText();
            setAcceptButtonEnabled(name != null  &&  name.length() > 0);
        }
	}
    
	public void setTemporaryModelerOverride() {
	    tmpModelerOverride = true;
	}
	    
    /**
    @since 2.0
    */
    protected boolean accept() {
        //save or open depending on the type of panel which was constructed
        boolean filenameEmpty = false;
        String fileName = filenameTextField.getText();
        if (fileName == null  ||  fileName.trim().equals("")) { //$NON-NLS-1$

            filenameEmpty = true;
        } else {
            fileName = fileName.trim();
            if (filenameAlreadyExists(fileName)) {
                selectedDirectoryEntry = existingDirectoryEntry;
            }
        }
        if (!acceptsFolders  &&  selectedDirectoryEntry != null  &&  directoryEntryView.allowsChildren(selectedDirectoryEntry)) {
            parentDirectoryEntry = selectedDirectoryEntry;
            selectedDirectoryEntry = null;
            expand();
        } else {
            if (panelType == TYPE_SAVE  &&  !filenameEmpty) {
                return save();
            }
            if (panelType == TYPE_OPEN  &&  !filenameEmpty) {
                return open();
            }
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Indicates whether folders may be valid target selections (in OPEN mode only).
    @return True if folders may be valid target selections
    @since 2.1
    */
    public boolean acceptsFolders() {
        return acceptsFolders;
    }

    /**
    @since 2.0
    */
    public void addChangeListener(final ChangeListener listener) {
        listenerList.add(ChangeListener.class, listener);
    }

    /**
    Recreates the list and table and redisplays the appropriate one
    @since 2.0
    */
    private void changeView() {
        Runnable runnable = new Runnable() {
            public void run() {
                changeView2();
            }
        };
        
        try {
            StaticUtilities.invokeLaterSafe(runnable);
        } catch (Exception e) {
            
        }
    }
    
    /**
     * @since 3.0
     */
    protected void changeView2() {
        createTable();
        createList();

        if(showingTable) {
            centralPanel.setViewportView(table);
        } else {
            centralPanel.setViewportView(list);
        }
		updateAcceptButton();

        this.revalidate();
        this.repaint();

    }

    /**
    Build the visual components for this Dialog.
    @since 2.0
    */
    private void createComponent() {
        parentDirectoryEntry = homeDirectoryEntry;

        final UIDefaults dflts = UIDefaults.getInstance();
        final Icon newOnIcon = dflts.getIcon("FileChooser.newFolderIcon"); //$NON-NLS-1$
        final Icon listButtonIcon = dflts.getIcon("FileChooser.listViewIcon"); //$NON-NLS-1$
        final Icon detailsButtonIcon = dflts.getIcon("FileChooser.detailsViewIcon"); //$NON-NLS-1$

        Border border5 = BorderFactory.createEmptyBorder(5,5,5,5);

        panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBorder(border5);

        topDetailsPanel = new JPanel();
        topDetailsPanel.setLayout(new BoxLayout(topDetailsPanel, BoxLayout.Y_AXIS));
        topDetailsPanel.setAlignmentX((float) 0.5);

        panel.add(topDetailsPanel);

        JPanel topPanel = new JPanel();
        topPanel.setLayout(new BoxLayout(topPanel, BoxLayout.X_AXIS));
        topPanel.setBorder(border5);

        LabelWidget lookInLabel = new LabelWidget("Look In: "); //$NON-NLS-1$
        lookInLabel.setBorder(border5);

        folderComboBox = new JComboBox();
        folderComboBox.setRenderer(new IconComboRenderer());
        populateFolderComboBox();

        //the button to return to the previous level
        upButton = new ButtonWidget();
        upButton.setIcon(new Icon() {
            public void paintIcon(Component c, Graphics g, int x, int y) {
                g.translate(x, y);

                // Fill background
                if (c.isEnabled()) {
                    g.setColor(javax.swing.plaf.metal.MetalLookAndFeel.getPrimaryControl());
                } else {
                    g.setColor(new Color(0x99, 0x99, 0x99));
                }
                g.fillRect(3,5, 12,9);

                // Draw outside edge of folder
                g.setColor(javax.swing.plaf.metal.MetalLookAndFeel.getPrimaryControlInfo());
                g.drawLine(1,6,    1,14); // left
                g.drawLine(2,14,  15,14); // bottom
                g.drawLine(15,13, 15,5);  // right
                g.drawLine(2,5,    9,5);  // top left
                g.drawLine(10,6,  14,6);  // top right
                // Draw the UP arrow
                if (!c.isEnabled()) {
                    g.setColor(new Color(0x66, 0x66, 0x66));
                }
                g.drawLine(8,13,  8,16); // arrow shaft
                g.drawLine(8, 9,  8, 9); // arrowhead top
                g.drawLine(7,10,  9,10);
                g.drawLine(6,11, 10,11);
                g.drawLine(5,12, 11,12);
        
                // Draw inner folder highlight
                g.setColor(javax.swing.plaf.metal.MetalLookAndFeel.getPrimaryControlHighlight());
                g.drawLine( 2,6,  2,13); // left
                g.drawLine( 3,6,  9,6);  // top left
                g.drawLine(10,7, 14,7);  // top right

                // Draw tab on folder
                g.setColor(javax.swing.plaf.metal.MetalLookAndFeel.getPrimaryControlDarkShadow());
                g.drawLine(11,3, 15,3); // top
                g.drawLine(10,4, 15,4); // bottom
        
                g.translate(-x, -y);
            }
            public int getIconWidth() {
                return 18;
            }
            public int getIconHeight() {
                return 18;
            }
        });
        if (((ComboData)folderComboBox.getSelectedItem()).isRoot()) {
            upButton.setEnabled(false);
        } else {
            upButton.setEnabled(true);
        }

        upButton.setToolTipText("Up one level"); //$NON-NLS-1$
        upButton.setBorderPainted(false);
        upButton.setFocusPainted(false);
        ButtonBorderPainter.registerButton(upButton);

        //the button to create a new folder
        newFolderButton = new ButtonWidget();
        newFolderButton.setIcon(newOnIcon);
        newFolderButton.setBorderPainted(false);
        newFolderButton.setToolTipText("Create new folder"); //$NON-NLS-1$
        newFolderButton.setFocusPainted(false);
        if (allowFolderCreation) {
            newFolderButton.setEnabled(true);
        } else {
            newFolderButton.setEnabled(false);
        }
        ButtonBorderPainter.registerButton(newFolderButton);

        //the toolbar containing the directory navigation, folder creation and display options controls
        JToolBar topToolBar = new JToolBar();
        topToolBar.setFloatable(false);
        topToolBar.setBorderPainted(false);
        topToolBar.addSeparator();
        topToolBar.add(upButton);
        topToolBar.addSeparator();
        if(showNewFolderButton){
            topToolBar.add(newFolderButton);
            topToolBar.addSeparator();
        }

        //the button to control whether a list or details table is displayed
        final ComboButtonWidget detailsComboButtonWidget = new ComboButtonWidget(new String[]{"List view", "Details view"}); //$NON-NLS-1$ //$NON-NLS-2$
        detailsComboButtonWidget.setCycleButtonToolTipText("Cycle views"); //$NON-NLS-1$
        detailsComboButtonWidget.setCycleButtonIcon(detailsButtonIcon);
        detailsComboButtonWidget.setPopupListButtonToolTipText("Display views"); //$NON-NLS-1$
        detailsComboButtonWidget.setVisible(true);
        topToolBar.add(detailsComboButtonWidget);

        topPanel.add(lookInLabel);
        topPanel.add(folderComboBox);
        topPanel.add(topToolBar);
        
        panel.add(topPanel);

        //create table here because you need it's background color
        table = new DirectoryEntryTable(directoryEntryView);
        table.setTableArray(parentDirectoryEntry);
        centralPanel = new JScrollPane();
        //set the viewport to a customized JViewport which will paint the TableViewport background to any color we
        //wish, in this case it's the color of the table's background.
        centralPanel.setViewport(new TableViewport());
        centralPanel.getViewport().setBackground(table.getBackground());

        panel.add(centralPanel);

        JPanel fileNamePanel = new JPanel();
        fileNamePanel.setLayout(new BoxLayout(fileNamePanel, BoxLayout.X_AXIS));

        LabelWidget fileNameLabel = new LabelWidget("Name: "); //$NON-NLS-1$
        fileNameLabel.setBorder(border5);
        fileNamePanel.add(fileNameLabel);

        //the textField where selected files are displayed or desired files are typed in
        filenameTextField = new TextFieldWidget() {
            public void setText(final String text) {
                if (nameFldUpdating) {
                    return;
                }
                super.setText(text);
            }
        };
        filenameTextField.setRequestFocusEnabled(true);
        filenameTextField.setEditable(true);
        filenameTextField.registerKeyboardAction(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                final ButtonWidget button = getAcceptButton();
                if (button.isEnabled()) {
                    activateAcceptButton();
                }
            }
        }, KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), WHEN_FOCUSED);
        fileNamePanel.add(filenameTextField);

        JPanel fileTypePanel = new JPanel();
        fileTypePanel.setLayout(new BoxLayout(fileTypePanel, BoxLayout.X_AXIS));

        fileTypeLabel = new LabelWidget("Type: "); //$NON-NLS-1$
        fileTypeLabel.setBorder(border5);
        fileTypePanel.add(fileTypeLabel);

        //the combo box containing the selectable extension filters
        filterComboBox = new JComboBox();
        filterComboBox.setEditable(false);
        filterComboBox.setRenderer(new FilterComboBoxRenderer());
        if (chooserFilters != null) {
            for (int i=0; i<chooserFilters.length; i++) {
                filterComboBox.addItem(chooserFilters[i]);
            }
        }

        //a filter which allows all entries thru (*.*)
        passThruDirectoryEntryFilter = new PassThroughDirectoryEntryFilter();
        filterComboBox.addItem(passThruDirectoryEntryFilter);
        filterComboBox.setSelectedIndex(0);
        selectedFilter = (DirectoryEntryFilter)filterComboBox.getItemAt(0);
        directoryEntryView.setFilter(selectedFilter);

        fileTypePanel.add(filterComboBox);

        JPanel filesPanel = new JPanel();
        filesPanel.setLayout(new BoxLayout(filesPanel, BoxLayout.Y_AXIS));
        filesPanel.setBorder(border5);
        filesPanel.add(fileNamePanel);
        filesPanel.add(Box.createVerticalStrut(10));
        if(showFilterComboBox){
            filesPanel.add(fileTypePanel);
        }

        //the button allowing additional information about the file to be displayed
        detailsButton = new ButtonWidget("Details"); //$NON-NLS-1$
        detailsButton.setPreferredSize(getAcceptButton().getPreferredSize());
        detailsButton.setEnabled(false);

        topButtonsPanel = new JPanel();
        topButtonsPanel.setLayout(new BoxLayout(topButtonsPanel, BoxLayout.X_AXIS));
        topButtonsPanel.setBorder(border5);
        populateTopButtonsPanel();

        JPanel bottomButtonsPanel = new JPanel();
        bottomButtonsPanel.setLayout(new BoxLayout(bottomButtonsPanel, BoxLayout.X_AXIS));
        bottomButtonsPanel.setBorder(border5);
        if (showDetailsButton) {
            bottomButtonsPanel.add(detailsButton);
        }

        JPanel buttonsPanel = new JPanel();
        buttonsPanel.setLayout(new BoxLayout(buttonsPanel, BoxLayout.Y_AXIS));
        buttonsPanel.add(topButtonsPanel);
        buttonsPanel.add(bottomButtonsPanel);


        JPanel bottomPanel = new JPanel();
        bottomPanel.setLayout(new BoxLayout(bottomPanel, BoxLayout.X_AXIS));
        bottomPanel.setBorder(border5);
        bottomPanel.add(filesPanel);
        bottomPanel.add(buttonsPanel);

        panel.add(bottomPanel);

        bottomDetailsPanel = new JPanel();
        bottomDetailsPanel.setLayout(new BoxLayout(bottomDetailsPanel, BoxLayout.Y_AXIS));
        bottomDetailsPanel.setAlignmentX((float) 0.5);

        panel.add(bottomDetailsPanel);

        newFolderButton.addActionListener(new ActionListener(){
            //create a new folder underm the current selected directory
            public void actionPerformed(ActionEvent e){
            	boolean bHaveRefreshed = false;
            	
                String fullMessage = "Current Folder: " + parentDirectoryEntry.getFullName(); //$NON-NLS-1$
                if (treeNodeEditor.isReadOnly(parentDirectoryEntry)) {
                    //if the current directory is read-only, throw an error message dialog
                    JOptionPane.showMessageDialog(ModifiedDirectoryChooserPanel.this, fullMessage + " is read-only", "Read-Only Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                } else {
                    //spawn a panel to input the new folder name
                    String newNodeName = JOptionPane.showInputDialog(ModifiedDirectoryChooserPanel.this, fullMessage,
                                                            "New Folder", JOptionPane.INFORMATION_MESSAGE); //$NON-NLS-1$
                    if ( newNodeName != null ) {
                        newNodeName = newNodeName.trim();
                        if ( newNodeName.length() > 0 ) {
	                        //create the new DirectoryEntry
	                        final DirectoryEntryEditor editor = directoryEntryView.getDirectoryEntryEditor();
	                        final UserTransaction xaction = editor.createWriteTransaction(ModifiedDirectoryChooserPanel.this);
	                        boolean wasErr = true;
	                        try {
	                            xaction.begin();
	                            editor.create(parentDirectoryEntry, newNodeName, DirectoryEntry.TYPE_FOLDER);
	                            xaction.commit();
	                            wasErr = false;
	                        } catch (final TransactionException err) {
	                            JOptionPane.showMessageDialog(ModifiedDirectoryChooserPanel.this, "Could not create entry " + newNodeName, "Creation Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
	                        } finally {
	                            try {
	                                if (wasErr) {
	                                    xaction.rollback();
	                                } else {
	                                	// make the new folder visible
	                                	//  1. find the row that contains our new folder
	                                	changeView();
	                                	bHaveRefreshed = true;
	                                	TableModel tmModel = table.getModel();                                	
	                                	
	                                	int iNewFoldersRow = -1;
	                                	int iRowCount = tmModel.getRowCount();
	                                	for ( int iRow = 0; iRow < iRowCount; iRow++ ) {
											FileSystemEntry fse = (FileSystemEntry)tmModel.getValueAt( iRow, 0 );		  										
											String sTempName = fse.getName();
		  
	                                		if( sTempName.trim().equals( newNodeName.trim() ) ) {
	                                			iNewFoldersRow = iRow;	
	                                			break;
	                                		}	
	                                	}
	                                	
	                                	// 2. if we find the row, display it
	                                	if ( iNewFoldersRow > -1 ) {
	                                		
	                                		int iCorrectedRow = table.convertRowIndexToView( iNewFoldersRow );
		                                	table.setRowSelectionInterval( iCorrectedRow, iCorrectedRow );	
		                                	int iY = iCorrectedRow * table.getRowHeight();
		                                	int iH = 3 * table.getRowHeight();
		                                	table.scrollRectToVisible( new Rectangle( 0, iY, 10, iH ) );
	                                	} else {
	                                	
	                                	}	                                	                                		                                	
	                                }
	                            } catch (final TransactionException err) {
	                                JOptionPane.showMessageDialog(ModifiedDirectoryChooserPanel.this, "Could not rollback creation of entry " + newNodeName, "Rollback Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
	                            }
	                        }
                        }
                //if editor.create() returned null, then the filename contains illegal characters,
                //or the disk is full, or there is a file IO error, or...
                    //refresh the view
    	                if ( !bHaveRefreshed ) {
	    	                changeView();
            	        }
                    }
                    
                }
            }
        });

        upButton.addActionListener(new ActionListener(){
            //move to the previous (next level up) directory
            public void actionPerformed(ActionEvent e){
                //folderComboBox has a listener that fires when its index is changed
                folderComboBox.setSelectedIndex(folderComboBox.getSelectedIndex()-1);
            }
        });

        detailsComboButtonWidget.getChoiceMenuItem("List view").addActionListener(new ActionListener(){ //$NON-NLS-1$
            //select the display to be in list form
            public void actionPerformed(ActionEvent e){
                    //set the columnHeaderView to null or it will stay even though the view has changed
                    centralPanel.setColumnHeaderView(null);
                    centralPanel.setViewportView(list);
                    detailsComboButtonWidget.setCycleButtonIcon(listButtonIcon);
                    //keep track of which type of view is currently showing
                    showingTable = false;
            }
        });

        detailsComboButtonWidget.getChoiceMenuItem("Details view").addActionListener(new ActionListener(){ //$NON-NLS-1$
            //select the display to be in table form with details
            public void actionPerformed(ActionEvent e){
                    centralPanel.setViewportView(table);
                    detailsComboButtonWidget.setCycleButtonIcon(detailsButtonIcon);
                    //keep track of which type of view is currently showing
                    showingTable = true;
            }
        });

        folderComboBox.addActionListener(new ActionListener() {
            //the pull-down folder hierarchy combo box listener
            public void actionPerformed(ActionEvent ev){
            	folderComboBoxSelectionChanged();
            }
        });

        getAcceptButton().addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                if (!(event instanceof WidgetActionEvent)) {
                    return;
                }
                final WidgetActionEvent widgetEvent = (WidgetActionEvent)event;
                if (widgetEvent.isProcessing()) {
                    return;
                }
                widgetEvent.setProcessing(true);
                boolean accepted = false;
                try {
                    accepted = accept();
                } finally {
                    if (!accepted) {
                        widgetEvent.destroy();
                    }
                    widgetEvent.setProcessing(false);
                }
            }
        });

        getCancelButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //clear the FilenameTextField on exit
                filenameTextField.setText(""); //$NON-NLS-1$
            }
        });

        filterComboBox.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e){
                //get and set the directoryEntryView to the selected TreeNodeFilter
                selectedFilter = (DirectoryEntryFilter)filterComboBox.getSelectedItem();
                directoryEntryView.setFilter(selectedFilter);
                changeView();
            }
        });

        detailsButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                //gets the nearest Window ancestor from DialogPanel.  To resize the DirectoryChooser panel
                //you must resize the parent window.
                Window parentWindow = getWindowAncestor();
                if (parentWindow != null) {
                    int currentWidth = parentWindow.getSize().width;
                    int currentHeight = parentWindow.getSize().height;
                    if(bottomDetailsPanel.isVisible()) {
                        //for now, I'm just enlargening/shrinking the parent window height by 200
                        //detailsPanel.setPreferredSize(new Dimension(0,0));
                        //detailsPanel.setMinimumSize(new Dimension(0,0));
                        parentWindow.setSize(new Dimension(currentWidth,currentHeight-200));
                        //detailsPanel.setVisible(false);
                    } else {
                        //detailsPanel.setPreferredSize(new Dimension(750,200));
                        //detailsPanel.setMinimumSize(new Dimension(200,200));
                        parentWindow.setSize(new Dimension(currentWidth,currentHeight+200));
                        //detailsPanel.setVisible(true);
                    }
                }
            }
        });

        filenameTextField.getDocument().addDocumentListener(new DocumentListener(){
            //monitor whether something has been typed in the filenameTextField box
            boolean wasEmpty = true;
            boolean textMatchedSelection;
            public void changedUpdate(final DocumentEvent event){
			}
            public void insertUpdate(final DocumentEvent event){
				if (wasEmpty) {
                    fireChangeEvent();
                    wasEmpty = false;
                }
                textMatchedSelection = false;
                update();
                if (textMatchedSelection) {
                    if (showingTable) {
                        if (table.getSelectedRowCount() == 1) {
                            table.scrollRectToVisible(table.getCellRect(table.getSelectedRow(), 0, true));
                        }
                    } else if (list.getSelectedValues().length == 1) {
                        list.ensureIndexIsVisible(list.getSelectedIndex());
                    }
                }
            }
            public void removeUpdate(final DocumentEvent event){
				if (event.getDocument().getLength() == 0) {
                    fireChangeEvent();
                    wasEmpty = true;
                }
                update();
            }
            private void update() {
                if (panelType == TYPE_OPEN) {   // Some of this could be done for save mode
                    nameFldUpdating = true;
                    Collection selections;
                    if (showingTable) {
                        selections = table.getSelectedObjects();
                    } else {
                    	// OLD: 
                    	//   this line led to an UnsupportedOperationException later when we tried
                    	//     to: selections.remove(entry);
                    	//   I believe the reason is that the List we get from Arrays.asList
                    	//   does not really implement remove...the array is NOT modifiable.
                        //selections = Arrays.asList(list.getSelectedValues());
                        // NEW: so do this instead:
                        selections = new ArrayList( Arrays.asList( list.getSelectedValues() ) );
                    }
                    final String text = filenameTextField.getText().trim();
                    final java.util.List children = directoryEntryView.getChildren(getParentDirectoryEntry());
                    String name, entryName;
                    DirectoryEntry entry;
                    int row, ndx, tableRow;
                    final DirectoryEntryFilter filter = getSelectedFilter();
                    boolean match;
                    for (final StringTokenizer textIter = new StringTokenizer(text, ";");  textIter.hasMoreTokens();) { //$NON-NLS-1$
                        name = textIter.nextToken().trim();
                        final Iterator entryIter = children.iterator();
                        for (row = 0;  entryIter.hasNext();  ++row) {
                            entry = (DirectoryEntry)entryIter.next();
                            entryName = entry.getName();
                            match = entryName.equalsIgnoreCase(name);
                            for (ndx = filter.getExtensionCount();  !match  &&  --ndx >= 0;) {
                                match = entryName.equalsIgnoreCase(name + '.' + filter.getExtension(ndx));
                            }
                            if (match) {
                                textMatchedSelection = true;
                                if (selections.contains(entry)) {
                                    selections.remove(entry);
                                } else {
                                    if (showingTable) {
                                        tableRow = table.convertRowIndexToView(row);
                                        table.addRowSelectionInterval(tableRow, tableRow);
                                        selectTableRow(false);
                                    } else {
                                        list.addSelectionInterval(row, row);
                                        selectListRow(false);
                                    }
                                }
                                break;
                            }
                        }
                    }
                    final Iterator entryIter = children.iterator();
                    for (row = 0;  entryIter.hasNext();  ++row) {
                        if (selections.contains(entryIter.next())) {
                            if (showingTable) {
                                tableRow = table.convertRowIndexToView(row);
                                table.removeRowSelectionInterval(tableRow, tableRow);
                                selectTableRow(false);
                            } else {
                                list.removeSelectionInterval(row, row);
                                selectListRow(false);
                            }
                        }
                    }
                    nameFldUpdating = false;
                }
				updateAcceptButton();
            }
        });

        //set view to details table by default
        centralPanel.setViewportView(table);
        //match the detailsComboButtonWidget to the default view
        detailsComboButtonWidget.setSelectedChoice("Details view"); //$NON-NLS-1$
        changeView();

        Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
        setLocation((screen.width - this.getPreferredSize().width)/2, (screen.height - this.getPreferredSize().height)/2 );
    }

	protected void folderComboBoxSelectionChanged() {
        int index = folderComboBox.getSelectedIndex();
        parentDirectoryEntry = ((ComboData)folderComboBox.getSelectedItem()).getDirectoryEntry();
        if (!acceptsFolders) {
            selectedDirectoryEntry = null;
        } else {
            selectedDirectoryEntry = parentDirectoryEntry;
            LogManager.logDetail("WORKSPACE",  //$NON-NLS-1$
                           "[ModifiedDirectoryChooserPanel.folderComboBox.actionPerformed] About to mod selectedDirectoryEntries: " + selectedDirectoryEntry ); //$NON-NLS-1$
            
			selectedDirectoryEntries = Arrays.asList(new Object[] {selectedDirectoryEntry});

            setNameFieldValueToParentName( parentDirectoryEntry );
            //old: filenameTextField.setText(parentDirectoryEntry.getName());
        }
        changeView();

        //enable/disable the upButton based on whether or not the currently selected DirectoryEntry is
        //a system root or a user-defined root
        if (((ComboData)folderComboBox.getSelectedItem()).isRoot()) {
            upButton.setEnabled(false);
        } else {
            upButton.setEnabled(true);
        }

        //now that you've made a change to the folderComboBox, clean it up if you've moved up a level
        int selectedIndex = folderComboBox.getSelectedIndex();
        for(int i = index + 1; i < folderComboBox.getItemCount(); i++) {
            if(i > selectedIndex) {
                if (!((ComboData)folderComboBox.getItemAt(i)).isRoot()) {
                    //remove items in the list past where you currently sit, unless the item is a root
                    folderComboBox.removeItemAt(i);
                    i--;
                }
            }
        }
	}
	
	protected void checkOpenStateListenerNotifying() {
		if (panelType == TYPE_OPEN) {
			if (openStateListener != null) {
				int docLength = filenameTextField.getDocument().getLength();
				String text = null;
				try {
					text = filenameTextField.getDocument().getText(0,
							docLength).trim();
				} catch (Exception ex) {
					//cannot occur
				}
				int selectedRow = table.getSelectedRow();
				boolean validSelectedRow = (selectedRow >= 0);
				boolean validText = (text.length() > 0);
				boolean validSelection = (validSelectedRow && validText);
				openStateListener.fileSelectionIsValid(validSelection);
			}
		}
	}
	
	protected void setNameFieldValueToParentName( DirectoryEntry parentDirectoryEntry ) {
        
        DirectoryEntryEditor editor = directoryEntryView.getDirectoryEntryEditor();
        if (editor.isReadOnly(parentDirectoryEntry)) {
	     	filenameTextField.setText( null );
	     	filenameTextField.setEditable( false );
        } else {
	     	filenameTextField.setText(parentDirectoryEntry.getName());        	
	     	filenameTextField.setEditable( true );
        }
	}                    



    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates the list of DirectoryEntries to be displayed.
    @since 2.0
    */
    private void createList() {

        list = new JList();
        list.setVisibleRowCount(100);

        //set custom renderer for displaying entry type icons before entry names
        boolean usingIconComboRenderer;
        if (listCellRenderer == null) {
        	list.setCellRenderer(new IconComboRenderer());
        	usingIconComboRenderer = true;
        } else {
            //this renderer would be set by DialogManager when a ModifiedDirectoryChooserPanel is created in the modeler
        	list.setCellRenderer(listCellRenderer);
        	usingIconComboRenderer = false;
        }


        //create treeNodes array sized to the number of entries in the directory
        DirectoryEntry[] treeNodes = new DirectoryEntry[directoryEntryView.getChildren(parentDirectoryEntry).size()];

        //get the list of entries
        Iterator iter = directoryEntryView.getChildren(parentDirectoryEntry).iterator();
        int treeDex = 0;
        //
        //create the aray of DirectoryEntries
        while (iter.hasNext()){
            treeNodes[treeDex] = (DirectoryEntry)iter.next();
            treeDex++;
        }

        //order the array, folders first
        treeNodes = orderNodes(treeNodes);
        
        if (usingIconComboRenderer) {
        	//convert the DirectoryEntry array to an array of ComboData.  ComboData keeps track of whether the
        	//entry is a root, leaf or in the middle without having to re-read the system drives.
        	ComboData[] comboDataList = new ComboData[treeNodes.length];
        	for (int i=0; i<treeNodes.length; i++) {
            	if (directoryEntryView.allowsChildren(treeNodes[i])) {   //node has no children (root?)
                	//set ComboData type to folder by default constructor
                	comboDataList[i] = new ComboData(1, treeNodes[i]);
            	} else {
                	//set ComboData type to leaf
                	comboDataList[i] = new ComboData(1, treeNodes[i], ComboData.TYPE_LEAF);
            	}
        	}
			//populate the list
        	list.setListData(comboDataList);
        } else {
        	list.setListData(treeNodes);
        }

        if (multiSelectionAllowed && panelType == TYPE_OPEN) {
            list.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        } else {
            list.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        }

        listMouseListener = new MouseAdapter() {
            //add mouse events for selecting from the list
            public void mouseClicked(MouseEvent e) {
                selectListRow(e.getClickCount() >= 2);
            }
        };
        list.addMouseListener(listMouseListener);
    }

    /**
	 * @since 3.0
	 */
	protected void selectListRow(final boolean doubleClicked) {
        LogManager.logDetail("WORKSPACE",  //$NON-NLS-1$
                                   "[ModifiedDirectoryChooserPanel.selectListRow] TOP, doubleClicked is: " + doubleClicked ); //$NON-NLS-1$
        int row = list.getSelectedIndex();
        if(row == -1 ) {
            selectedDirectoryEntry = null;
		            LogManager.logDetail("WORKSPACE",  //$NON-NLS-1$
                                   "[ModifiedDirectoryChooserPanel.selectListRow] About to set selectedDirectoryEntries to EMPTY " ); //$NON-NLS-1$
            
            selectedDirectoryEntries = Collections.EMPTY_LIST;
            filenameTextField.setText(""); //$NON-NLS-1$
            return;
        }

        //get the selected DirectoryEntry
		DirectoryEntry treeNode;
		if (list.getSelectedValue() instanceof DirectoryEntry) {
			treeNode = (DirectoryEntry)list.getSelectedValue();
		} else {
			treeNode = ((ComboData)list.getSelectedValue()).getDirectoryEntry();
		}
		
        selectedDirectoryEntry = treeNode;

        // jh fix (1/30/2003)
		selectedDirectoryEntries = Arrays.asList(new Object[] {selectedDirectoryEntry});
		
        String treeNodeName = treeNode.getName();
        if (acceptsFolders  ||  !directoryEntryView.allowsChildren(treeNode)) {

            if (doubleClicked  &&  directoryEntryView.allowsChildren(treeNode)) {

                parentDirectoryEntry = selectedDirectoryEntry;
                if (!acceptsFolders) {
                    selectedDirectoryEntry = null;
                } else if (filenameSelectionAllowed) {
                    filenameTextField.setText(treeNodeName);
                }
                expand();
                return;
            }
            //selected DirectoryEntry has no children (leaf)
            detailsButton.setEnabled(true);
            if(filenameSelectionAllowed) {
                //place DirectoryEntry name in the filenameTextField
                filenameTextField.setText(treeNodeName);
            }
			updateAcceptButton();
            //if the selected entry is a leaf (file) and double-clicked, perform the accept button operation
            if (doubleClicked) {
                activateAcceptButton();
            }
        } else {
            //selected DirectoryEntry is a folder
            detailsButton.setEnabled(false);
            
            //NOTE: don't clear the name field when a directory is selected
            // jh note: try putting this line back (was commented out per
            //          prev comment)
            filenameTextField.setText(null);


            if (doubleClicked) {

                parentDirectoryEntry = treeNode;
                selectedDirectoryEntry = null;
                expand();

                // jh 2/19/2002: try doing this after the expand:
                filenameTextField.setText(""); //$NON-NLS-1$
            } else {
                selectedDirectoryEntry = treeNode;
                
                // jh fix (1/30/2003)
				selectedDirectoryEntries = Arrays.asList(new Object[] {selectedDirectoryEntry});
                
            }
        }
	}

    /**
    Creates the table of DirectoryEntries with each DirectoryEntry's properties displayed along with it.
    @since 2.0
    */
    private void createTable() {
        //create the table model to construct the details table.
        table = new DirectoryEntryTable(directoryEntryView);
        table.setTableArray(parentDirectoryEntry, propertiesToShow);
        table.setAutoResizeMode(TableWidget.AUTO_RESIZE_ALL_COLUMNS);
        table.setShowGrid(false);
        table.setSortable(true);
        table.setRowSelectionAllowed(true);
        table.setColumnSortedAscending((EnhancedTableColumn)table.getEnhancedColumnModel().getColumn(0));
        table.sizeColumnsToFitData( 100 );
        
        //TODO: set renderer on "Name" column rather than column 0
        if (tableCellRenderer != null) {
			table.getColumnModel().getColumn(0).setCellRenderer(tableCellRenderer);
        }

        if (multiSelectionAllowed && panelType == TYPE_OPEN) {
            table.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        } else {
            table.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        }

        tableMouseListener = new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                selectTableRow(e.getClickCount() >= 2);
            }
        };
        table.addMouseListener(tableMouseListener);
        tableSelectionListener = new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent lse) {
            	int row = table.getSelectedRow();
                if(row == -1){
                	if (!nameFldUpdating) {
                    	selectedDirectoryEntry = null;
		            LogManager.logDetail("WORKSPACE",  //$NON-NLS-1$
                                   "[ModifiedDirectoryChooserPanel.tableSelectionListener.valueChanged] About to set selectedDirectoryEntries to EMPTY " ); //$NON-NLS-1$
                    	
                    	selectedDirectoryEntries = Collections.EMPTY_LIST;
                    	filenameTextField.setText(""); //$NON-NLS-1$
                	}
                    return;
                }
                //mouse clicking in the table returns the DirectoryEntry for the row clicked
                DirectoryEntry treeNode = (DirectoryEntry)table.getSelectedObject();

                Collection treeNodes = table.getSelectedObjects();

                if (acceptsFolders  ||  !directoryEntryView.allowsChildren(treeNode)) {

                    String nodeNames = ""; //$NON-NLS-1$
                    Iterator selectedIter = treeNodes.iterator();
                    int semiFlag = 0;
                    while (selectedIter.hasNext()) {
                        DirectoryEntry de = (DirectoryEntry)selectedIter.next();
                        if (semiFlag == 0) {
                            nodeNames = nodeNames + de.getName();
                        } else {
                            nodeNames = nodeNames + "; " + de.getName(); //$NON-NLS-1$
                        }
                        semiFlag++;
                    }
	            	LogManager.logDetail("WORKSPACE",  //$NON-NLS-1$
                               "[ModifiedDirectoryChooserPanel.tableSelectionListener.valueChanged] About to set selectedDirectoryEntries to treeNodes " + treeNodes); //$NON-NLS-1$
                    
                    selectedDirectoryEntries = treeNodes;
                    selectedDirectoryEntry = treeNode;
                    detailsButton.setEnabled(true);
                    //filenameTextField.setText(treeNodeName);
                    if (filenameSelectionAllowed) {
                        filenameTextField.setText(nodeNames);
                    }
                } else {
                    //selectedDirectoryEntry is a folder
                    detailsButton.setEnabled(false);
                    //NOTE: don't clear the name field when a directory is selected in a SAVE panel
                    if (panelType == TYPE_OPEN) {
                        filenameTextField.setText(null);
                    }
                }
				updateAcceptButton();
            }
        };
    	table.addListSelectionListener(tableSelectionListener);
    }

    /**
	 * @since 3.0
	 */
	protected void selectTableRow(final boolean doubleClicked) {
        int row = table.getSelectedRow();
        if(row == -1){
        	if (!nameFldUpdating) {
            	selectedDirectoryEntry = null;
		        LogManager.logDetail("WORKSPACE",  //$NON-NLS-1$
                                   "[ModifiedDirectoryChooserPanel.selectTableRow] About to set selectedDirectoryEntries to EMPTY " ); //$NON-NLS-1$
            	
            	selectedDirectoryEntries = Collections.EMPTY_LIST;
            	filenameTextField.setText(""); //$NON-NLS-1$
        	}
            return;
        }
        //mouse clicking in the table returns the DirectoryEntry for the row clicked
        DirectoryEntry treeNode = (DirectoryEntry)table.getSelectedObject();
		updateAcceptButton();
        if (acceptsFolders  ||  !directoryEntryView.allowsChildren(treeNode)) {
            //selectedDirectoryEntry is a leaf (file)
            if (doubleClicked) {
                if ( directoryEntryView.allowsChildren(treeNode)) {
                    parentDirectoryEntry = selectedDirectoryEntry;
                    if (!acceptsFolders) {
                        selectedDirectoryEntry = null;
                    }
                    expand();
                } else {
                    //if a selected directory entry is double-clicked, perform the acceptButton action on it
                    activateAcceptButton();
                }
            } 
        } else {
            if (doubleClicked) {
                parentDirectoryEntry = treeNode;
                selectedDirectoryEntry = null;
                expand();
            } else {
                selectedDirectoryEntry = treeNode;
            }
        }
	}

    /**
    Executed when the accept button (Open) is pressed with a folder selected.  This opens the folder and adds
    it to the folderComboBox.
    @since 2.0
    */
    protected void expand() {
        int index = folderComboBox.getSelectedIndex();
        //track number of parents the selected entry has.  Passed to ComboData to control indent
        //displayed by IconComboRenderer
        int parentCount = 0;

        if (!rootsList.contains(parentDirectoryEntry)) {
            //DirectoryEntry is not a root (which has parentCount of 0)
            parentCount = 1;
            //count the parents
            DirectoryEntry tnParent = (DirectoryEntry)directoryEntryView.getParent(parentDirectoryEntry);

            while (tnParent != null && !rootsList.contains(tnParent) ) {
                //keep getting and counting parents until parent is a root

                // jh 2/18/2002: fixed the following; old code looped
                tnParent = (DirectoryEntry) directoryEntryView.getParent(tnParent);
                if ( tnParent != null ) {
                    parentCount++;
                }
            }
        }

        //convert DirectoryEntry and add it to the folderComboBox.  parentCount is used for indenting
        //in IconComboRenderer
        folderComboBox.insertItemAt(new ComboData(parentCount, parentDirectoryEntry), index+1);
        //set the folderComboBox item selected to the folder you just double-clicked on
        folderComboBox.setSelectedIndex(index + 1);
        changeView();
    }

    public boolean isFolderComboSelectionARoot() {
        return  ((ComboData)folderComboBox.getSelectedItem()).isRoot();
    }

    public DirectoryEntry getFolderComboBoxSelection() {
        DirectoryEntry deResult = null;
        
        deResult = ((ComboData)folderComboBox.getSelectedItem()).getDirectoryEntry();

        return deResult;
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Search DirectoryEntry names in the current folder for a match already existing
    @since 2.0
    */
    protected boolean filenameAlreadyExists(String filename) {
        if ( acceptsFolders  &&  
             selectedDirectoryEntry != null && 
             selectedDirectoryEntry == parentDirectoryEntry ) {
            existingDirectoryEntry = selectedDirectoryEntry;
            return true;
        }

        //get local copy of directoryEntryView for filtering purposes
        DirectoryEntryView dev = directoryEntryView;
        //set filter to passthru so files are found even when a filter is set
        dev.setFilter(passThruDirectoryEntryFilter);
        final java.util.List childlist = dev.getChildren(parentDirectoryEntry);
        Iterator iter = childlist.iterator();
        while (iter.hasNext()){
            DirectoryEntry de = (DirectoryEntry)(iter.next());
            if (de.getName().toLowerCase().equals(filename.toLowerCase())) {

                existingDirectoryEntry = de;
                return true;
            }
        }

        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void fireChangeEvent() {
        final Object[] listeners = listenerList.getListenerList();
        ChangeEvent event = null;
        for (int ndx = listeners.length - 2;  ndx >= 0;  ndx -= 2) {
            if (listeners[ndx] == ChangeListener.class) {
                if (event == null) {
                    event = new ChangeEvent(filenameTextField);
                }
                ((ChangeListener)listeners[ndx + 1]).stateChanged(event);
            }
        }
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns the folder combo box
    @return The folder combo box
    @since 2.1
    */
    protected JComboBox getFolderComboBox() {
        return folderComboBox;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns the root of the current directory.
    @return The root of the current directory
    @since 2.1
    */
    public TreeNode getCurrentRoot() {
        int ndx = folderComboBox.getSelectedIndex();
        ComboData node = (ComboData)folderComboBox.getItemAt(ndx);
        while (ndx > 0  &&  !node.isRoot()) {
            node = (ComboData)folderComboBox.getItemAt(--ndx);
        }
        return node.getDirectoryEntry();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns the root of the DirectoryEntryView
    @since 2.0
    */
    public DirectoryEntry getRoot() {
        return homeDirectoryEntry;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    private Collection getRootsToUse() {
        Collection rootsToUse = new ArrayList();

        Collection allRootsList = directoryEntryView.getRoots();
        if (rootsToShow != Collections.EMPTY_LIST && rootsToShow != null) {
            Iterator rootIter = allRootsList.iterator();
            while (rootIter.hasNext()) {
                DirectoryEntry de = (DirectoryEntry) rootIter.next();
                if (rootsToShow.contains(de)) {
                    rootsToUse.add(de);
                }
            }
        } else {
            rootsToUse = allRootsList;
        }

        return rootsToUse;
    }
    
    /**
    @since 2.1
    */
    public String getNameFieldText() {
        return filenameTextField.getText();
    }
    
    /**
    @since 2.1
    */
    public DirectoryEntry getParentDirectoryEntry() {
        return parentDirectoryEntry;
    }
        
    /**
    Get the selected DirectoryEntry from this dialog.
    @since 2.0
    */
    public TreeNode getSelectedTreeNode() {
        return selectedDirectoryEntry;
    }

    /**
    Get the selected DirectoryEntries from this dialog when a multiple selection is made.
    @since 2.0
    */
    public Collection getSelectedTreeNodes() {
        LogManager.logDetail("WORKSPACE",  //$NON-NLS-1$
                             "[ModifiedDirectoryChooserPanel.getSelectedTreeNodes] About to return selectedDirectoryEntries: " + selectedDirectoryEntries ); //$NON-NLS-1$
    	
        return selectedDirectoryEntries;
    }

    /**
    Get the selected DirectoryEntries from this dialog when a multiple selection is made.
    @since 2.0
    */
    public void setPanelType(int type) {
        panelType = type;
        if (panelType == TYPE_OPEN) {
            getAcceptButton().setText("Open"); //$NON-NLS-1$
        } else {
            getAcceptButton().setText("Save"); //$NON-NLS-1$
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Executed when the accept button (Open) is pressed with a panelType of OPEN
    @since 2.0
    */
    protected boolean open() {
        String newFilename = filenameTextField.getText();

        //if multiple selection is allowed, assume the user just wants the collection of selected nodes returned.
        //No filename checking, no filename creation if not existing, no checking for existing filenames...
        if (multiSelectionAllowed) {
            return true;
        }

        //WidgetActionEvent.isProcessing(true) suspends the DialogWindow from executing it's AcceptButton routine until ModifiedDirectoryChooserPanel
        //is finished with it.  When done with the event, DialogWindow will dispose.
        if (!filenameAlreadyExists(newFilename)) {
            //Check to see if the filename is valid before proceeding any further
            DirectoryEntryEditor editor = directoryEntryView.getDirectoryEntryEditor();
            int yesNoResult = JOptionPane.showConfirmDialog(this, newFilename + " not found. Create?", newFilename + " not found", JOptionPane.YES_NO_OPTION); //$NON-NLS-1$ //$NON-NLS-2$
            if (yesNoResult == JOptionPane.YES_OPTION) {
		
				// 
                if (editor.isReadOnly(parentDirectoryEntry)) {
                    JOptionPane.showMessageDialog(this, PROTECTED_PARENT_MSG, "Read only folder", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$
                    //kick out if the name is no good
                    return false;
                }

                if (!editor.isNameValid(newFilename)) {
                    JOptionPane.showMessageDialog(this, INVALID_FILENAME_MSG, "Invalid Name", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$
                    //kick out if the name is no good
                    return false;
                }

                // automatically tack on an extension, if needed
                newFilename = addFileExtension( newFilename, selectedFilter );

                final UserTransaction xaction = editor.createWriteTransaction(this);
                boolean wasErr = true;
                try {
                    xaction.begin();
                    DirectoryEntry de = (DirectoryEntry)editor.create(parentDirectoryEntry, newFilename, DirectoryEntry.TYPE_FILE);
                    //if editor.create() returned null, then the filename contains illegal characters,
                    //or the disk is full, or there is a file IO error, or...
                    if (de != null) {
                        editor.makeExist(de);
                        selectedDirectoryEntry = de;
                    } else {
                        JOptionPane.showMessageDialog(this, "Could not create entry " + newFilename, "Creation Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                        return false;
                    }
                    xaction.commit();
                    wasErr = false;
                } catch (final TransactionException err) {
                    JOptionPane.showMessageDialog(this, "Could not create entry " + newFilename, "Creation Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                } finally {
                    try {
                        if (wasErr) {
                            xaction.rollback();
                        }
                    } catch (final TransactionException err) {
                        JOptionPane.showMessageDialog(this, "Could not rollback creation of entry " + newFilename, "Rollback Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                }
            } else {
                // Kill the event so DialogWindow will not dispose.
                return false;
            }
        } else {
            selectedDirectoryEntry = existingDirectoryEntry;
        }
        return true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Reorders the directory entries so entries allowing children (folders) come before
    entries not allowing children (files)
    @since 2.0
    */
    private DirectoryEntry[] orderNodes(Object[] nodeList){
        //vector of nodes allowing children
        Vector nodes = new Vector();
        //vector of nodes not allowing children
        Vector leaves = new Vector();
        for (int q=0; q<nodeList.length; q++){
            DirectoryEntry tn = (DirectoryEntry)nodeList[q];
            if(directoryEntryView.allowsChildren(tn)){
                nodes.addElement(nodeList[q]);
            } else {
                leaves.addElement(nodeList[q]);
            }
        }
        //combine nodes and leaves
        nodes.addAll(leaves);

        //cast the vector into a DirectoryEntry[] array
        DirectoryEntry[] orderedNodeList = new DirectoryEntry[nodes.size()];
        for (int k=0; k<nodes.size(); k++){
            orderedNodeList[k] = (DirectoryEntry)nodes.elementAt(k);
        }
        nodeList = orderedNodeList;
        return orderedNodeList;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Initialize the folderComboBox with its root entries
    @since 2.0
    */
    private void populateFolderComboBox() {
        //get the directoryEntryView roots (system drives)
        Iterator iter = rootsList.iterator();
        //get the home directory entry
        homeDirectoryEntry = (DirectoryEntry)directoryEntryView.getHome();
        int folderIndex = -1;
        //track where in the list the home directory entry is located
        int folderHomeEntryIndex = 0;
        while (iter.hasNext()) {
            DirectoryEntry de = (DirectoryEntry)iter.next();
            if(directoryEntryView.isAncestorOf(de, homeDirectoryEntry)){
                //if the root directory is the ancestor of the home entry, start the process of adding
                //the home entry and its parents under it's ancestral root
                folderComboBox.addItem(new ComboData(0, de, ComboData.TYPE_ROOT));
                folderIndex++;

                //if a home is set, find out how many levels away it is from the root
                int parentCount = 0;
                if (!rootsList.contains(homeDirectoryEntry)) {
                    //DirectoryEntry is not a root (which has parentCount of 0)
                    parentCount = 1;
                    //count the parents
                    DirectoryEntry tnParent = (DirectoryEntry)directoryEntryView.getParent(homeDirectoryEntry);
                    while (!rootsList.contains(tnParent)) {
                        //keep getting and counting parents until parent is a root
                        if (directoryEntryView.getParent(tnParent) != null) {
                            tnParent = (DirectoryEntry) directoryEntryView.getParent(tnParent);
                            parentCount++;
                        }
                    }
                }

                //TYPE_OTHER refers to folders
                folderComboBox.addItem(new ComboData(parentCount, homeDirectoryEntry, ComboData.TYPE_OTHER));
                folderIndex++;

                //working backwards from the home entry, start adding its parents
                int insertIndex = folderIndex;
                DirectoryEntry tempDirectoryEntry = (DirectoryEntry)directoryEntryView.getParent(homeDirectoryEntry);
                for (int i=parentCount-1; i>0; i--) {
                    folderComboBox.insertItemAt(new ComboData(i, tempDirectoryEntry, ComboData.TYPE_OTHER), insertIndex);
                    tempDirectoryEntry = (DirectoryEntry)directoryEntryView.getParent(tempDirectoryEntry);
                    folderIndex++;
                }

                //this is where the home directory is located
                folderHomeEntryIndex = folderIndex;
            } else {
                folderComboBox.addItem(new ComboData(0, de, ComboData.TYPE_ROOT));
                folderIndex++;
				if (directoryEntriesMatch(de, homeDirectoryEntry)) {
                    //this is where the home directory is located in the list of roots (system drives)
                    folderHomeEntryIndex = folderIndex;
                }
            }
        }
        //default to the home directory entry
        folderComboBox.setSelectedIndex(folderHomeEntryIndex);
    }

	private boolean directoryEntriesMatch(DirectoryEntry en1, DirectoryEntry en2) {
		boolean match = false;
		String type1 = en1.getType().toString();
		String type2 = en2.getType().toString();
		if (type1.equals(type2)) {
			boolean removingTrailingBackslash = false;
			if (type1.equalsIgnoreCase("Folder")) { //$NON-NLS-1$
				removingTrailingBackslash = true;
			}
			String fullName1 = en1.getFullName();
			String fullName2 = en2.getFullName();
			if (removingTrailingBackslash) {
				fullName1 = removeTrailingBackslash(fullName1);
				fullName2 = removeTrailingBackslash(fullName2);
			}
			if (fullName1.equals(fullName2)) {
				boolean namesMatch;
				String name1 = en1.getName();
				String name2 = en2.getName();
				if ((name1 == null) && (name2 == null)) {
					namesMatch = true;
				} else if ((name1 == null) || (name2 == null)) {
					namesMatch = false;
				} else {
					namesMatch = name1.equals(name2);
				}
				match = namesMatch;
			}
		}
		return match;
	}
	
	private String removeTrailingBackslash(String str) {
		String outputStr;
		int len = str.length();
		char lastChar = str.charAt(len - 1);
		if (lastChar == '\\') {
			outputStr = str.substring(0, len - 1);
		} else {
			outputStr = new String(str);
		}
		return outputStr;
	}
		
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Populates the topButtonsPanel, broken out to override when needed.
    @since 2.0
    */
    protected void populateTopButtonsPanel() {
        topButtonsPanel.add(Box.createHorizontalGlue());
        topButtonsPanel.add(getNavigationBar());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Executed when the window containing this panel is left and then returned to.
    @since 2.0
    */
    protected void reactivateRefresh(){
        changeView();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void removeChangeListener(final ChangeListener listener) {
        listenerList.remove(ChangeListener.class, listener);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Executed when the accept button is pressed with a panelType of SAVE
    @since 2.0
    */
    protected boolean save() {
        //WidgetActionEvent.isProcessing() suspends the DialogWindow from executing its AcceptButton routine until ModifiedDirectoryChooserPanel
        //is finished with it.  When done with the event, DialogWindow will dispose.
        //new filename condition
        String newFilename = filenameTextField.getText();

        //Check to see if the filename is valid before proceeding any further
        DirectoryEntryEditor editor = directoryEntryView.getDirectoryEntryEditor();
        if (!editor.isNameValid(newFilename)) {
            JOptionPane.showMessageDialog(this, INVALID_FILENAME_MSG, "Invalid Name", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$
            //kick out if the name is no good
            return false;
        }

        // automatically tack on an extension, if needed
        newFilename = addFileExtension( newFilename, selectedFilter );

        if (!filenameAlreadyExists(newFilename)) {
            final UserTransaction xaction = editor.createWriteTransaction(this);
            boolean wasErr = true;
            try {
                xaction.begin();
                DirectoryEntry de = (DirectoryEntry)editor.create(parentDirectoryEntry, newFilename, DirectoryEntry.TYPE_FILE);
                //if editor.create() returned null, then the filename contains illegal characters,
                //or the disk is full, or there is a file IO error, or...
                if (de != null) {
                    editor.makeExist(de);
                    selectedDirectoryEntry = de;
                } else {
                    JOptionPane.showMessageDialog(this, "Could not create entry " + newFilename, "Creation Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                    return false;
                }
                xaction.commit();
                wasErr = false;
            } catch (final TransactionException err) {
                JOptionPane.showMessageDialog(this, "Could not create entry " + newFilename, "Creation Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
            } finally {
                try {
                    if (wasErr) {
                        xaction.rollback();
                    }
                } catch (final TransactionException err) {
                    JOptionPane.showMessageDialog(this, "Could not rollback creation of entry " + newFilename, "Rollback Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                }
            }
       //filename already exists condition
        } else if (!tmpModelerOverride) {
            //spawn YES/NO overwrite dialog
            int yesNoResult = JOptionPane.showConfirmDialog(this, existingDirectoryEntry.getName() + " already exists. Overwrite?", existingDirectoryEntry.getName() + " already exists", JOptionPane.YES_NO_OPTION); //$NON-NLS-1$ //$NON-NLS-2$
            //overwrite file condition
            if (yesNoResult == JOptionPane.YES_OPTION) {
                //read-only file condition
                if (treeNodeEditor.isReadOnly(existingDirectoryEntry)) {
                    JOptionPane.showMessageDialog(this, existingDirectoryEntry.getName() + " is read-only", "Read-Only Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                    return false;
                //proceed with overwrite
                }
                final UserTransaction xaction = editor.createWriteTransaction(this);
                boolean wasErr = true;
                try {
                    xaction.begin();
                    editor.delete(existingDirectoryEntry);
                    DirectoryEntry de = (DirectoryEntry)editor.create(parentDirectoryEntry, newFilename, DirectoryEntry.TYPE_FILE);
                    //if editor.create() returned null, then the filename contains illegal characters,
                    //or the disk is full, or there is a file IO error, or...
                    if (de != null) {
                        editor.makeExist(de);
                        selectedDirectoryEntry = de;
                    } else {
                        JOptionPane.showMessageDialog(this, "Could not create entry " + newFilename, "Creation Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                        return false;
                    }
                    xaction.commit();
                    wasErr = false;
                } catch (final TransactionException err) {
                    JOptionPane.showMessageDialog(this, "Could not create entry " + newFilename, "Creation Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                } finally {
                    try {
                        if (wasErr) {
                            xaction.rollback();
                        }
                    } catch (final TransactionException err) {
                        JOptionPane.showMessageDialog(this, "Could not rollback creation of entry " + newFilename, "Rollback Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                }
            //do not overwrite file condition
            } else {
                // Kill the event so DialogWindow will not dispose.
                return false;
            }
        } else {
            selectedDirectoryEntry = existingDirectoryEntry;
        }
        return true;
    }


    protected String addFileExtension( String sFileName, DirectoryEntryFilter selectedFilter ) {
        
        // If the selected filter is not a passthru filter ...
        if ( !(selectedFilter instanceof PassThroughDirectoryEntryFilter) ) {
            // If the selected filter is one used with XMI model or project files and
            // the new filename does not currently have one of the supported extensions
            // then add the extension defined with the filter.
            if ( FileResourceConstants.isValidXMIFileExtension(selectedFilter.getExtension(0)) && !FileResourceConstants.hasXMIFileExtension(sFileName) ) {
                if ( !sFileName.endsWith(FileResourceConstants.EXTENSION_DELIMITER) ) {
                    sFileName += FileResourceConstants.EXTENSION_DELIMITER + selectedFilter.getExtension(0);
                } else {
                    sFileName += selectedFilter.getExtension(0);
                }
            } 
            // If the user has not entered a dot in the new filename, add the 
            // the first extension of the selected filter.  If a dot has been 
            // entered, assume the user knows what he's doing.
            else if (sFileName.indexOf(FileResourceConstants.EXTENSION_DELIMITER) < 0) {
                sFileName += FileResourceConstants.EXTENSION_DELIMITER + selectedFilter.getExtension(0);
            }
        }
        return sFileName;

//        //if the user has not entered a dot in the new filename, and the filter is not a passthru filter, add
//        //the first extension of the selected filter to the filename.  If a dot has been entered, assume the
//        //user knows what he's doing.
//        if (  sFileName.indexOf('.') == -1
//        && !( selectedFilter instanceof PassThroughDirectoryEntryFilter ) ) {
//            // (meaning no dot in filename, not a passthru filter)
//
//            sFileName += "." + selectedFilter.getExtension(0);
//        }
//        return sFileName;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Set the label for the accept button.
    @since 2.0
    */
    public void setAcceptButtonLabel(String s){
//        acceptButtonLabel = s;
        getAcceptButton().setText(s);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether folders may be valid target selections (in OPEN mode only).  Double-clicking on a folder, however, will still
    merely expand that folder.  This may only be called before the panel is displayed.
    @param acceptsFolders True if folders may be valid target selections
    @since 2.1
    */
    public void setAcceptsFolders(final boolean acceptsFolders) {
        this.acceptsFolders = acceptsFolders;
        if (acceptsFolders) {
            selectedDirectoryEntry = parentDirectoryEntry;
        	filenameTextField.setText(selectedDirectoryEntry.getName());
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the newFolder button is displayed.
    @since 2.0
    */
    public void setAllowFolderCreation(boolean b){
        allowFolderCreation = b;
        newFolderButton.setEnabled(b);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the newFolder button is displayed.
    @since 2.0
    */
    public void setAllowNonFolderCreation(boolean b){
//        allowNonFolderCreation = b;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Set the label for the cancel button.
    @since 2.0
    */
    public void setCancelButtonLabel(String s){
//        cancelButtonLabel = s;
        getCancelButton().setText(s);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Set the label for the details button.
    @since 2.0
    */
    public void setDetailsButtonLabel(String s){
        detailsButton.setText(s);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Set whether or not the user is allowed to select/type in his own filename.  Setting this false disables
    all file selection in both the list and table views and disables all the navigation/file selection buttons
    and combo boxes.
    @since 2.0
    */
    public void setFilenameSelectionAllowed(boolean allowed){
        filenameSelectionAllowed = allowed;
        filenameTextField.setEnabled(allowed);
        changeView();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Set the DirectoryEntry where you want the ModifiedDirectoryChooserPanel to start.
    @since 2.0
    */
    public void setInitialFilename(String name){
        filenameTextField.setText(name);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Set the message that appears inside the Invalid File message dialog panel
    @since 2.0
    */
    public void setInvalidFilenameMsg(String msg) {
        INVALID_FILENAME_MSG = msg;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Set whether or not multiple file selections can be made in an OPEN panel type.
    @since 2.0
    */
    public void setMultiSelection(boolean allowed){
        multiSelectionAllowed = allowed;
        changeView();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the Accept button is displayed.
    @since 2.0
    */
    public void setShowAcceptButton(boolean b){
        getAcceptButton().setVisible(b);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the Cancel button is displayed.
    @since 2.0
    */
    public void setShowCancelButton(boolean b){
        getCancelButton().setVisible(b);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the newFolder button is displayed.
    @since 2.0
    */
    public void setShowDetailsButton(boolean b){
        detailsButton.setVisible(b);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the newFolder button is displayed.
    @since 2.0
    */
    public void setShowFilterComboBox(boolean b){
        fileTypeLabel.setVisible(b);
        filterComboBox.setVisible(b);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the newFolder button is displayed.
    @since 2.0
    */
    public void setShowNewFolderButton(boolean b){
        newFolderButton.setVisible(b);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Control whether or not the (*.* All Files) is included in the list of filters.
    If this is the only filter, setShowPassThruFilter(false) WILL NOT remove it.
    @since 2.0
    */
    public void setShowPassThruFilter(boolean show){
        boolean passThruFilterExists = false;
        int passThruFilterExistsAt = -1;
        for (int i=0; i<filterComboBox.getItemCount(); i++) {
            //check all filters to see if one is a passThruFilter
            if (filterComboBox.getItemAt(i) instanceof PassThroughTreeNodeFilter) {
                passThruFilterExists = true;
                passThruFilterExistsAt = i;
            }
        }
        if (show && !passThruFilterExists) {
            //if a passThruFilter does not exist but you want one...
            filterComboBox.addItem(passThruDirectoryEntryFilter);
            changeView();
        }
        if (!show && passThruFilterExists) {
            //if a passThruFilter exists and you don't want one...
           if (filterComboBox.getItemCount() > 1) {
                //remove the filter from where it exists
                filterComboBox.removeItemAt(passThruFilterExistsAt);
                filterComboBox.setSelectedIndex(0);
                selectedFilter = (DirectoryEntryFilter)filterComboBox.getItemAt(0);
                directoryEntryView.setFilter(selectedFilter);
                changeView();
            } else {
            }
        }
//        showPassThruFilter = show;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /* Get the table used in the table view.  Used by DialogManager to set the renderer.
     * @since 3.0
     */
    public PropertiedObjectArrayTable getTable() {
        return table;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /* Get the list used in the list view.  Used by DialogManager to set the renderer.
     * @since 3.0
     */
    public JList getList() {
        return list;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /* Set the table renderer to be used when the ModifiedDirectoryChooserPanel is created from the Modeler.  The default
     * renderer is set by the DirectoryEntryTable and simply shows either a folder icon or a 
     * generic file icon.
     * @since 3.0
     */
    public void setModelerTableCellRenderer(TableCellRenderer renderer) {
        if (renderer != null && table != null) {
    		this.tableCellRenderer = renderer;
    		if(table.getColumnModel().getColumnCount()>0) {
    			table.getColumnModel().getColumn(0).setCellRenderer(tableCellRenderer);
    		}
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /* Set the list renderer to be used when the ModifiedDirectoryChooserPanel is created from the Modeler.  The default
     * renderer is set by the DirectoryEntryTable and simply shows either a folder icon or a 
     * generic file icon.
     * @since 3.0
     */
    public void setModelerListCellRenderer(ListCellRenderer renderer) {
        if (renderer != null && list != null) {
    		this.listCellRenderer = renderer;
    		list.setCellRenderer(listCellRenderer);
        }
    }
    
    public DirectoryEntryFilter getSelectedFilter(){
    	return this.selectedFilter;	
    }
}

/**
 * FilterComboBoxRenderer is used by the filterComboBox display filter names and show selection highlighting.
 */
class FilterComboBoxRenderer extends JLabel implements ListCellRenderer {
     public FilterComboBoxRenderer() {
         setOpaque(true);
     }
     public Component getListCellRendererComponent(
         JList list,
         Object value,
         int index,
         boolean isSelected,
         boolean cellHasFocus)
     {
         TreeNodeFilter tnf = (TreeNodeFilter)value;
         setText(tnf.getDescription());
         setBackground(isSelected ? Color.blue : Color.white);
         setForeground(isSelected ? Color.white : Color.black);
         return this;
     }
 }

/**
 * ComboData is used to wrap around DirectoryEntries and supply additional information which would be to cumbersome
 * to extract from the DirectoryEntryView every time it is needed.
 */
class ComboData  {

    protected int index;
    protected DirectoryEntry directoryEntry;
    protected static int TYPE_LEAF = 1;
    protected static int TYPE_ROOT = 0;
    protected static int TYPE_OTHER = -1;
    protected int type = -1;


    /**
    * Constructor without a type.  Index is used by IconCombRenderer to determine how much to indent entries.
    * Root entries should have an index of 0 and all other entries should be > 0.  There are only 2 types which we
    * are concerned with, TYPE_ROOT and TYPE_LEAF.  By default, type is TYPE_OTHER.
    */
    public ComboData(int index, DirectoryEntry directoryEntry){
        this(index, directoryEntry, -1);
    }

    /**
    * Constructor with a type.
    */
    public ComboData(int index, DirectoryEntry directoryEntry, int type){
        this.index = index;
        this.directoryEntry = directoryEntry;
        this.type = type;
    }

    /**
    * Return the index.  Used for indenting in IconComboRenderer.
    */
    public int getIndex() {
        return index;
    }

    /**
    * Return true if a leaf.  Used for icon selection in IconComboRenderer.
    */
    public boolean isLeaf() {
        if (type == TYPE_LEAF) {
            return true;
        }
        return false;
    }

    /**
    * Return true if a root.  Used for icon selection in IconComboRenderer.
    */
    public boolean isRoot() {
        if (type == TYPE_ROOT) {
            return true;
        }
        return false;
    }

    /**
    * Return the DirectoryEntry.
    */
    public DirectoryEntry getDirectoryEntry() {
        return directoryEntry;
    }

    /**
    * Return the DirectoryEntry name.  Get the fullName if the DirectoryEntry is a root so it will display (i.e. "C:\")
    */
    public String toString() {
        if (directoryEntry.getName().equals("") || directoryEntry.getName().equals(null)) { //$NON-NLS-1$
            return directoryEntry.getFullName();
        }
        return directoryEntry.getName();
    }
}

/**
 * IconComboRenderer is used to display DirectoryEntry icons and names in the display list and the folderComboBox.
 */
class IconComboRenderer	extends JLabel implements ListCellRenderer {

    final Icon diskIcon = UIDefaults.getInstance().getIcon("FileView.hardDriveIcon"); //$NON-NLS-1$
    final Icon folderIcon = UIDefaults.getInstance().getIcon("Tree.closedIcon"); //$NON-NLS-1$
    final Icon elementIcon = UIDefaults.getInstance().getIcon("Tree.leafIcon"); //$NON-NLS-1$
    protected Color m_textSelectionColor;
    protected Color m_textNonSelectionColor;
    protected Color m_textNonselectableColor;
    protected Color m_bkSelectionColor;
    protected Color m_bkNonSelectionColor;
    protected Color m_borderSelectionColor;

    protected Color  m_textColor;
    protected Color  m_bkColor;

    protected boolean m_hasFocus;

    public IconComboRenderer() {
        super();
        m_textColor = m_textNonSelectionColor;
        m_bkColor = m_bkNonSelectionColor;
        setOpaque(false);
    }

    public Component getListCellRendererComponent(JList list,
                Object obj, int row, boolean sel, boolean hasFocus) {

        m_bkColor = list.getBackground();
        m_textColor = list.getForeground();
        m_bkSelectionColor = list.getSelectionBackground();
        m_textSelectionColor = list.getSelectionForeground();
        m_textNonSelectionColor = m_textColor;
        m_bkNonSelectionColor = m_bkColor;

        if (obj == null) {
            return this;
        }
        boolean selectable = true;

        if (obj instanceof ComboData) {
        	
			//cast to proper type
            ComboData comboData = (ComboData)obj;
            //display name properly, fullName() for a root, name() for all 
            //others
            String displayName = comboData.toString();
			setText(displayName);
            //set icons
            if (comboData.isLeaf()) {
                setIcon(elementIcon);
            } else if (comboData.isRoot()) {
                setIcon(diskIcon);
            } else {
                setIcon(folderIcon);
            }
        	Border b;
        	if (row == -1) {    //cell that displays as selected
            	b = (new EmptyBorder(0, 0, 0, 0));  //keep it justified left
        	} else {
            	b = (new EmptyBorder(0, comboData.getIndex()*16, 0, 0));    //cells in the list, step right if subfolder
        	}
        	//set the border, indenting to the right if index > 0
            setBorder(b);
        }

        setFont(list.getFont());
        m_textColor = (sel ? m_textSelectionColor :
                        (selectable ? m_textNonSelectionColor :
                            m_textNonselectableColor));
        m_bkColor = (sel ? m_bkSelectionColor :
                        m_bkNonSelectionColor);
        m_hasFocus = hasFocus;
        return this;
    }

    public void paint(Graphics g) {
        g.setColor(m_bkNonSelectionColor);
        g.fillRect(0, 0, getWidth(), getHeight());

        g.setColor(m_bkColor);
        int offset = 0;
        g.fillRect(offset, 0, getWidth() - 1 - offset, getHeight() - 1);

        if (m_hasFocus) {
            g.setColor(m_borderSelectionColor);
            g.drawRect(offset, 0, getWidth()-1-offset, getHeight()-1);
        }

        setForeground(m_textColor);
        setBackground(m_bkColor);
        super.paint(g);
    }

}

/**
 * TableViewport exists solely to paint the area under the table in the scrollpane the same color as the table's
 * background.  If you do not do this, you get a gray box under any table that does not fill the scrollPane.
 */
class TableViewport extends JViewport {
    public void paint(Graphics g) {
        g.setColor(getBackground());
        g.fillRect(0, 0, getWidth(), getHeight());
        super.paint(g);
    }
}
