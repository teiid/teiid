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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.table;

// JDK imports
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Iterator;

import javax.swing.BoxLayout;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.toolbox.ui.IconConstants;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.SpacerWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;

/**
@since 2.0
@version 2.0
@author John P. A. Verhaeg (substantial mods by Jerry Helbling)
*/
public abstract class AbstractTableOptionPanel extends JScrollPane
implements IconConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final String NONE = "<None>";

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private final TableWidget table;
    private String[] colNames = null;
    private JComponent colsPanel = new JPanel();

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public AbstractTableOptionPanel(final TableWidget table) {
        this.table = table;
        initializeAbstractTableOptionPanel();    
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected JComponent addColumnPanel(final int index) {
        final ColumnPanel colPanel = new ColumnPanel();
        colsPanel.add(colPanel, index);

        final ButtonWidget andButton = colPanel.getAndButton();
        andButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                handleAndAction(colPanel);
            }
        });

        final JComboBox colBox = colPanel.getColumnsComboBox();
        colBox.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                handleColumnSelected(colPanel);
            }
        });

        final ButtonWidget deleteButton = colPanel.getDeleteButton();
        deleteButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                handleDeleteAction(colPanel);
            }
        });

        customizeColumnPanel(colPanel, andButton, deleteButton);

        return colPanel;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void customizeColumnPanel(JComponent columnPanel, ButtonWidget andButton, ButtonWidget deleteButton) {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public EnhancedTableColumn getColumn(final int index) {
        try {
            return (EnhancedTableColumn)table.getColumn(((JComboBox)getColumnPanel(index).getComponent(1)).getSelectedItem());
        } catch (final IllegalArgumentException err) {
            return null;
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected JComponent getColumnPanel(final int index) {
        return (JComponent)colsPanel.getComponent(index);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getColumnCount() {
        return colsPanel.getComponentCount();
    }
    
    public String getColumnPanelSelection(int theIndex) {
        AbstractTableOptionPanel.ColumnPanel pnl = (AbstractTableOptionPanel.ColumnPanel)getColumnPanel(theIndex);
        JComboBox cbx = pnl.getColumnsComboBox();
        
        return (String)cbx.getSelectedItem();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TableWidget getTable() {
        return table;
    }
    
    protected void handleAndAction(ColumnPanel thePanel) {
        final Component[] comps = colsPanel.getComponents();

        for (int ndx = comps.length; --ndx >= 0;) {
            if (comps[ndx] == thePanel && (ndx == comps.length - 1  ||
                !((JComboBox)((JComponent)comps[ndx + 1]).getComponent(1)).getSelectedItem().equals(NONE))) {
                addColumnPanel(ndx + 1);
    
                // ok, try implementing NONE in each newly added panel
    
                //if ( comps.length == 2 ) {                    
                //    removeEntryFromComboBox( thePanel.getColumnsComboBox(), NONE );                    
                //}
                ColumnPanel cpnlLastPanel = getLastPanel();
                removeEntryFromComboBox( cpnlLastPanel.getColumnsComboBox(), NONE );
                addEntryToComboBox( cpnlLastPanel.getColumnsComboBox(), NONE );
                cpnlLastPanel.getColumnsComboBox().setSelectedItem( NONE );
    
                // TODO: reevaluate last panel
                LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.handleAndAction] About to call applyPoliciesToLastPanel");                
                applyPoliciesToLastPanel( null );
                revalidate();
                break;
            }
        }
    }
    
    protected void handleColumnSelected(ColumnPanel thePanel) {
        applyPoliciesToLastPanel( null );

        ButtonWidget andButton = thePanel.getAndButton();
        
        if ( thePanel.getColumnsComboBox().getSelectedItem() != null
            && !thePanel.getColumnsComboBox().getSelectedItem().equals(NONE) ) {
            LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.handleColumnSelected] Selected Item is NOT <NONE>");                
//            // who needs this?: 
            if (!andButton.isVisible()) {                             
                
                andButton.setVisible(true);
                thePanel.repaint();
            // who needs this.
            }
        } else { // equals NONE
            
            LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.handleColumnSelected] Selected Item IS <NONE>");                
            
            if (andButton.isVisible()) {
                andButton.setVisible(false);
                thePanel.repaint();
            }
        }
        // put this at the end:
        applyPoliciesToLastPanel( null );
    }

    protected void handleDeleteAction(ColumnPanel thePanel) {
        if (colsPanel.getComponentCount() == 1) {
            removeEntryFromComboBox( thePanel.getColumnsComboBox(), NONE );
            addEntryToComboBox( thePanel.getColumnsComboBox(), NONE );

            thePanel.getColumnsComboBox().setSelectedItem(NONE);
        } else {

            // Only send the colname of the deleted col IF IT WAS BEFORE
            //  the new 'last colname':  In other words, only send the colname
            //  if a row other than the last row was deleted.
            String sSelectedColNameFromDeletedPanel = null;
            if ( thePanel != getLastPanel() ) {
                sSelectedColNameFromDeletedPanel
                    = (String)thePanel.getColumnsComboBox().getSelectedItem();
            }
            
            colsPanel.remove(thePanel);
            
            // we could capture the selected entry of the panel we have removed
            //  and add it to the model of the newly made LAST            
            colsPanel.revalidate();
            colsPanel.repaint();            
            
            LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.handleDeleteAction] About to call applyPoliciesToLastPanel");                
            applyPoliciesToLastPanel( sSelectedColNameFromDeletedPanel );
        }
    }

    protected ColumnPanel getLastPanel() {
        final Component[] comps = colsPanel.getComponents();
        return (ColumnPanel)comps[comps.length - 1];
    }

    protected void applyPoliciesToLastPanel( String sDeletedColumnName ) {
        // 1. hide the 'and' button if the cbx only has 2 or fewer colnames in it
        // 2. if only one panel is left, disable the DELETE button
        
        final Component[] comps = colsPanel.getComponents();
        ColumnPanel colPanel = (ColumnPanel)comps[comps.length - 1];
        
        if ( colPanel != null ) {

        
            // if we have just deleted a previous panel, include its col name
            //  in the cbx of the last panel:
            if ( sDeletedColumnName != null ) {
                LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.applyPoliciesToLastPanel] About to add the deleted col name to the new last panel");
                
                addEntryToComboBox( colPanel.getColumnsComboBox(), sDeletedColumnName );
            }
 

            // start out by unconditionally showing and enabling the And button
            //LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.applyPoliciesToLastPanel] Only 1 entry showing and NONE is NOT selected; About to SHOW the And button");                                                    
            colPanel.getAndButton().setVisible(true);                      
            colPanel.getAndButton().setEnabled(true);        
            
            
            // if NONE selected, hide the And button        
            colPanel.getColumnsComboBox().setEnabled(true);
            if ( colPanel.getColumnsComboBox().getSelectedItem() != null  
                && colPanel.getColumnsComboBox().getSelectedItem().equals(NONE) ) {
                LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.applyPoliciesToLastPanel] NONE is selected, about to hide the And button");                                
                colPanel.getAndButton().setVisible(false);                      
                colPanel.getAndButton().setEnabled(false);        
            } else {            
                
                int iSize = colPanel.getColumnsComboBox().getModel().getSize();
                boolean bContainsNone = containsNone( colPanel.getColumnsComboBox() );
                
                if ( (iSize < 4 && bContainsNone) || (iSize < 3 && !bContainsNone) ) {               
                    LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.applyPoliciesToLastPanel] Fewer than 3 (4) cbx choices left, About to HIDE the And button");                
                    colPanel.getAndButton().setVisible(false);        
                }
            }
            
            colsPanel.revalidate();
            colsPanel.repaint(); 
        }           
    }
    
    private boolean containsNone( JComboBox cbx ) {
        boolean bFound = false;
        
        for ( int ix = 0; ix < cbx.getModel().getSize(); ix++ ) {
            String sElement = (String)cbx.getModel().getElementAt( ix );
            if ( sElement.equals( NONE ) ) {
                bFound = true;
                break;   
            }   
        }
        return bFound;
    }

    
    private void addEntryToComboBox( JComboBox cbx, String sNewEntry ) {
        
        //LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.applyPoliciesToLastPanel] About to add the deleted col name to the new last panel");                
        boolean bSaveCbxEnabledState = cbx.isEnabled();                
        Object oCurrentSelection = cbx.getModel().getSelectedItem();
        
        ComboBoxModel cbxmdlOld = cbx.getModel();
        
        ArrayList arylCurrentContents = new ArrayList( cbxmdlOld.getSize() );
        
        for ( int ix = 0; ix < cbxmdlOld.getSize(); ix++ ) {
            arylCurrentContents.add( cbxmdlOld.getElementAt( ix ) );   
        }
        arylCurrentContents.add( sNewEntry );
        ComboBoxModel cbxmdlNew = new DefaultComboBoxModel( arylCurrentContents.toArray() );
        cbx.setModel( cbxmdlNew );
        cbxmdlNew.setSelectedItem( oCurrentSelection );
        // Why should we change the enable state???: cbx.setEnabled( true );        
        cbx.setEnabled( bSaveCbxEnabledState );        
    }

    private void removeEntryFromComboBox( JComboBox cbx, String sEntry ) {
        
        LogManager.logDetail("HIDEPANEL", "[AbstractTableOptionPanel.applyPoliciesToLastPanel] About REMOVE this entry: " + sEntry );
        boolean bSaveCbxEnabledState = cbx.isEnabled();                
        Object oCurrentSelection = cbx.getModel().getSelectedItem();
        
        ComboBoxModel cbxmdlOld = cbx.getModel();
        
        ArrayList arylCurrentContents = new ArrayList( cbxmdlOld.getSize() );
        
        for ( int ix = 0; ix < cbxmdlOld.getSize(); ix++ ) {
            String sTemp = (String)cbxmdlOld.getElementAt( ix );
            // add all but the one we wish to drop to the array
            if ( !sTemp.equals( sEntry ) ) {
                arylCurrentContents.add( cbxmdlOld.getElementAt( ix ) );
            }   
        }
        
        ComboBoxModel cbxmdlNew = new DefaultComboBoxModel( arylCurrentContents.toArray() );
        cbx.setModel( cbxmdlNew );
        cbxmdlNew.setSelectedItem( oCurrentSelection );
        cbx.setEnabled( bSaveCbxEnabledState );        
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeColumnsPanel(JComponent columnPanel) {
        if (columnPanel == null) {
            columnPanel = addColumnPanel(0);
        }
        final Dimension size = columnPanel.getPreferredSize();
        getVerticalScrollBar().setUnitIncrement(size.height);
        size.height *= 3;
        getViewport().setPreferredSize(size);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeAbstractTableOptionPanel() {
        setViewportView(colsPanel);
        colsPanel.setLayout(new BoxLayout(colsPanel, BoxLayout.Y_AXIS));
        final EnhancedTableColumnModel colModel = table.getEnhancedColumnModel();
        colNames = new String[colModel.getHiddenAndShownColumnCount() + 1];
        colNames[0] = NONE;
        final Iterator iterator = colModel.getHiddenAndShownColumns().iterator();
        for (int ndx = 1;  iterator.hasNext();  ++ndx) {
            colNames[ndx] = (String)((EnhancedTableColumn)iterator.next()).getIdentifier();
        }
    }
    
    public class ColumnPanel extends JPanel {
        private ButtonWidget btnAnd;
        private ButtonWidget btnDelete;
        private JComboBox cbxColumns;
        
        public ColumnPanel() {
            setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
            setAlignmentX(0.0f);
            add(new LabelWidget("Column "));
            
            cbxColumns = new JComboBox(colNames);
            cbxColumns.setMaximumSize(cbxColumns.getPreferredSize());
            add(cbxColumns);
            
            btnAnd = new ButtonWidget(", and");
            btnAnd.setVisible(false);
            add(btnAnd);

            SpacerWidget spacer = SpacerWidget.createHorizontalSpacer();
            spacer.setMaximumSize(new Dimension(Short.MAX_VALUE, 0));
            add(spacer);

            btnDelete = new ButtonWidget(UIDefaults.getInstance().getIcon(DELETE_ICON_PROPERTY));
            add(btnDelete);
        }
        
        public ButtonWidget getAndButton() {
            return btnAnd;
        }
        
        public ButtonWidget getDeleteButton() {
            return btnDelete;
        }
        
        public JComboBox getColumnsComboBox() {
            return cbxColumns;
        }
    }
}
