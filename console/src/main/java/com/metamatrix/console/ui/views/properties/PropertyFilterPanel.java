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

package com.metamatrix.console.ui.views.properties;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.util.Collection;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.toolbox.ui.widget.*;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.TreeWidget;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;
   

public class PropertyFilterPanel extends BasePanel{
    public static final String LOGGING = "Logging Operation";
    private static String ALL_TAB = "all tab";

    private JSplitPane pnlControllSPP;
    private JPanel pnlSystemTree;
    private GridBagLayout flagLayout,BESPanelLayout,MRSPanelLayout;
    private JCheckBox allPropCB = new CheckBox("All Properties");
    private JRadioButton basicCB = new JRadioButton("Basic       ",true);
    private JRadioButton expertCB = new JRadioButton("Expert       ");
    private JRadioButton bothBECB = new JRadioButton("Both       ");

    private JRadioButton modifiableCB = new JRadioButton("Modifiable");
    private JRadioButton readOnlyCB = new JRadioButton("Read Only");
    private JRadioButton bothMRCB = new JRadioButton("Both       ",true);

    private ButtonGroup groupBE = new ButtonGroup();
    private ButtonGroup groupMR = new ButtonGroup();
    private NextStartupPanel nextStartupPanel;
    private PropertyFilter suFilter;
    
    private ConsolePropertiedEditor propEditor;
    private TreeWidget tree;
    private Collection propDefns;
    private PropertiesTreeModelFactory ptmFactory;
    private PropertiesMasterPanel masterPanel;
    private TreePath prevTP;
    private boolean runValueChange = true;
    private boolean canModifyServerProperties = false;

    public PropertyFilterPanel(PropertiesMasterPanel masterPanel,ConsolePropertiedEditor editor, boolean canModifyServerProperties) {
        super();
        this.masterPanel = masterPanel;
        this.canModifyServerProperties = canModifyServerProperties;
        propEditor = editor;
        createComponent();
    }
    
    public void createComponent() {
        StaticUtilities.startWait(this);
        this.setLayout(new BorderLayout());
        pnlSystemTree = new JPanel(new BorderLayout());
        JPanel pnlFlag = new JPanel();
        JPanel pnlBESelector = new JPanel();
        JPanel pnlMRSelector = new JPanel();

        flagLayout = new GridBagLayout();
        pnlFlag.setLayout(flagLayout);
        flagLayout.setConstraints(allPropCB, new GridBagConstraints(0, 0, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        flagLayout.setConstraints(pnlBESelector, new GridBagConstraints(0, 1, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));
        flagLayout.setConstraints(pnlMRSelector, new GridBagConstraints(0, 2, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 5, 5, 5), 0, 0));

        BESPanelLayout = new GridBagLayout();
        pnlBESelector.setLayout(BESPanelLayout);
        BESPanelLayout.setConstraints(basicCB, new GridBagConstraints(0, 0, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        BESPanelLayout.setConstraints(expertCB, new GridBagConstraints(0, 1, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        BESPanelLayout.setConstraints(bothBECB, new GridBagConstraints(0, 2, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        pnlBESelector.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createEtchedBorder(),
                        BorderFactory.createEmptyBorder(1,0,1,0)));
        pnlBESelector.add(basicCB);
        pnlBESelector.add(expertCB);
        pnlBESelector.add(bothBECB);
        groupBE.add(basicCB);
        groupBE.add(expertCB);
        groupBE.add(bothBECB);


        MRSPanelLayout = new GridBagLayout();
        pnlMRSelector.setLayout(MRSPanelLayout);
        MRSPanelLayout.setConstraints(modifiableCB, new GridBagConstraints(0, 0, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        MRSPanelLayout.setConstraints(readOnlyCB, new GridBagConstraints(0, 1, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        MRSPanelLayout.setConstraints(bothMRCB, new GridBagConstraints(0, 2, 1, 1, 0, 0,
                                            GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        pnlMRSelector.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createEtchedBorder(),
                        BorderFactory.createEmptyBorder(1,0,1,0)));

        pnlMRSelector.add(modifiableCB);
        pnlMRSelector.add(readOnlyCB);
        pnlMRSelector.add(bothMRCB);
        groupMR.add(modifiableCB);
        groupMR.add(readOnlyCB);
        groupMR.add(bothMRCB);

        TitledBorder tBorder;
        tBorder = new TitledBorder("Display");
        pnlFlag.setBorder(tBorder);

        pnlControllSPP = new Splitter(JSplitPane.VERTICAL_SPLIT,
                                        true,
                                        pnlSystemTree,
                                        pnlFlag);

        pnlFlag.add(allPropCB);
        pnlFlag.add(pnlBESelector);
        pnlFlag.add(pnlMRSelector);
        add(pnlControllSPP);
        pnlControllSPP.addComponentListener(new ComponentAdapter() {
                    public void componentResized(ComponentEvent e) {
                        Double sizeDB = new Double(pnlControllSPP.getSize().getHeight()-260);
                        pnlControllSPP.setDividerLocation(sizeDB.intValue());
                    }
                });
        nextStartupPanel = new NextStartupPanel(PropertiesMasterPanel.NEXT_STARTUP,
                PropertiesMasterPanel.NEXT_STARTUP_ICON, this,
                masterPanel.getConnection());
        suFilter = new PropertyFilter();        
        nextStartupPanel.setPropertiedEditor(propEditor);
        propDefns =  propEditor.getPropDefn();
        ptmFactory
            = new PropertiesTreeModelFactory(propDefns);
        TreeModel tm = ptmFactory.getTreeModel();
        tree = new TreeWidget(tm);
        tree.setRootVisible(false);
        tree.expandRow(0);
        StaticTreeUtilities.expandAll(tree);
        JScrollPane sp = new JScrollPane(tree);
        pnlSystemTree.add(sp, BorderLayout.CENTER);
        StaticUtilities.endWait(this);
        propControllProcess();
    }
    
    boolean isModifyServerProperties() {
        return canModifyServerProperties;
    }

    JRadioButton getMJRB() {
        return  modifiableCB;
    }

    JRadioButton getRJRB() {
        return  readOnlyCB;
    }

    JRadioButton getBJRB() {
        return  bothMRCB;
    }

    NextStartupPanel getNextStartupPanel() {
        return nextStartupPanel;
    }

    public void postRealize() {
    }
    
    private void propControllProcess() {
        tree.setSelectionRow(0);
        tree.getSelectionModel().addTreeSelectionListener(
             new TreeSelectionListener() {
                public void valueChanged(TreeSelectionEvent se) {
                    if (!runValueChange) {
                        runValueChange = true;
                        return;
                    }
                    if (masterPanel.havePendingChanges()) {
                        boolean proceeding = masterPanel.finishUp();
                        if (!proceeding) {
                        // cancel return
                        runValueChange = false;
                        tree.setSelectionPath(prevTP);
                        return;
                        }
                    }
                    prevTP = tree.getSelectionPath();
                    DefaultTreeNode treeNode = (DefaultTreeNode)prevTP.getLastPathComponent();
                    String title = propEditor.getCurrentTitle();
                    String gn = treeNode.getName();
                    Object propDefns = treeNode.getContent();
                        
                    if (title.equals(PropertiesMasterPanel.NEXT_STARTUP)) {
                        nextStartupPanel.setGroupName(gn, propEditor.getNSUDefns((java.util.List)propDefns), suFilter);
                    } 

                    //clear  hashmap that contain property value change  nspFilter, opFilter, suFilter
                    if (nextStartupPanel.getPropertiedEditor().getChangeHM() != null)
                        nextStartupPanel.getPropertiedEditor().getChangeHM().clear();
                }
            }
        );
        CheckBoxListener cBoxListener = new CheckBoxListener();
        allPropCB.addActionListener(cBoxListener);
        basicCB.addActionListener(cBoxListener);
        bothBECB.addActionListener(cBoxListener);
        expertCB.addActionListener(cBoxListener);
        modifiableCB.addActionListener(cBoxListener);
        bothMRCB.addActionListener(cBoxListener);
        readOnlyCB.addActionListener(cBoxListener);
    }

	public void refresh() {
        refresh(ALL_TAB);
    }

    public void refresh(String tabIndex) {

        TreePath currentSelectPath = tree.getSelectionPath();
        String gn = null;
        Object propDefns = null;
        if (currentSelectPath != null) {
            DefaultTreeNode treeNode = (DefaultTreeNode)currentSelectPath.getLastPathComponent();
            gn = treeNode.getName();
            propDefns = treeNode.getContent();  //TODO: GET defferent Defin from Tree Node.
        }
        if (gn == null) {
            //should not come here
            if (tabIndex.equals(PropertiesMasterPanel.NEXT_STARTUP)|| tabIndex.equals(ALL_TAB)) {
                nextStartupPanel.setGroupName(ConsolePropertyObjectId.ALL_SYS_PROPS, null, suFilter);
            }
        } else {
            if (tabIndex.equals(PropertiesMasterPanel.NEXT_STARTUP)) {
                nextStartupPanel.setGroupName(gn, propEditor.getNSUDefns((java.util.List)propDefns), suFilter);
            }
        }
    }
    
    void setMRBStatus(boolean isDisable) {
        if (isDisable) {
            suFilter.setIsMRBEnabled(false);
        } else {
            suFilter.setIsMRBEnabled(true);
        }
        if (isDisable || this.allPropCB.isSelected()) {
            modifiableCB.setEnabled(false);
            readOnlyCB.setEnabled(false);
            bothMRCB.setEnabled(false);
        } else {
            modifiableCB.setEnabled(true);
            readOnlyCB.setEnabled(true);
            bothMRCB.setEnabled(true);
        }
    }
    
    private void setAllCheckBoxesEnableStatus(boolean status) {
        basicCB.setEnabled(status);
        expertCB.setEnabled(status);
        bothBECB.setEnabled(status);

        if (!canModifyServerProperties) {
            return;
        }

        modifiableCB.setEnabled(status);
        readOnlyCB.setEnabled(status);
        bothMRCB.setEnabled(status);
    }

//    private String getTreePathString(JTree tree) {
//        Object[] gnObjs =tree.getSelectionPath().getPath();
//        StringBuffer sb = new StringBuffer();
//        int i = 0;
//        for(; i <gnObjs.length-1; i++) {
//            sb.append(gnObjs[i].toString());
//            sb.append(".");
//        }
//        sb.append(gnObjs[i]);
//        return sb.toString();
//    }

  
class CheckBoxListener implements ActionListener {

    public  CheckBoxListener() {
        super();
    }
    
    public void actionPerformed(ActionEvent e) {
		String title = propEditor.getCurrentTitle();
		Object source = e.getSource();
        if (source == allPropCB) {
            suFilter.setAllProperties(allPropCB.isSelected());
            if (allPropCB.isSelected()) {
                setAllCheckBoxesEnableStatus(false);
            } else {
                setAllCheckBoxesEnableStatus(true);
            }
        } else if (source == basicCB) {
            suFilter.setBasicProperties(basicCB.isSelected());
        } else if (source == expertCB) {
            suFilter.setExpertProperties(expertCB.isSelected());
        } else if (source == bothBECB) {
            suFilter.setBothBEProperties(bothBECB.isSelected());
        } else if (source == modifiableCB) {
            suFilter.setModifiableProperties(modifiableCB.isSelected());
        } else if (source == readOnlyCB) {
            suFilter.setReadOnlyProperties(readOnlyCB.isSelected());
        } else if (source == bothMRCB) {
            suFilter.setBothMRProperties(bothMRCB.isSelected());
        }
		refresh(title);
    }
}

}
