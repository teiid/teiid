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

package com.metamatrix.console.ui.views.vdb;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Arrays;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;

import com.metamatrix.console.ui.util.ActionFireableButtonWidget;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableModel;

public class EntitlementMigrationReportSummaryPanel extends JPanel {
    public final static int NAME_COLUMN_NUM = 0;
    public final static int MATCHED_COLUMN_NUM = 1;
    public final static int DROPPED_COLUMN_NUM = 2;
    public final static int NEW_COLUMN_NUM = 3;
    public final static int DETAILS_COLUMN_NUM = 4;

    private ActionFireableButtonWidget[] detailButtons;
    private JButton saveButton;
    private EntitlementMigrationSummaryInfo[] summaryInfo;
    private JTable table;
    private EntitlementMigrationInfoSource infoSource;
    private String sourceVDB;
    private int sourceVDBVersion;
    private String targetVDB;
    private int targetVDBVersion;

    public EntitlementMigrationReportSummaryPanel(EntitlementMigrationSummaryInfo[] info,
            EntitlementMigrationInfoSource source, String sVDB, int sVDBVersion,
            String tVDB, int tVDBVersion) {
        super();
        summaryInfo = info;
        infoSource = source;
        sourceVDB = sVDB;
        sourceVDBVersion = sVDBVersion;
        targetVDB = tVDB;
        targetVDBVersion = tVDBVersion;
        init();
    }

    private void init() {
        //NOTE-- column headers here must be in order matching NAME_COLUMN_NUM, etc. above
        com.metamatrix.toolbox.ui.widget.table.DefaultTableModel model =
                new com.metamatrix.toolbox.ui.widget.table.DefaultTableModel(
                new Vector(Arrays.asList(new String[] {
                        "Role Name", //$NON-NLS-1$
                        "# Matched", //$NON-NLS-1$
                        "# Dropped", //$NON-NLS-1$
                        "# New", //$NON-NLS-1$
                        ""})), 0); //$NON-NLS-1$
        table = new MigrationSummaryTableWidget(this, model);
        JScrollPane tableSP = new JScrollPane(table);
        add(tableSP);
        saveButton = new JButton("Save All Details to File"); //$NON-NLS-1$
        add(saveButton);
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        layout.setConstraints(tableSP, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(3, 3, 3, 3), 0, 0));
        layout.setConstraints(saveButton, new GridBagConstraints(0, 1,
                1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.NONE, new Insets(10, 10, 5, 10), 0, 0));
        insertData();
    }

    private void insertData() {
        detailButtons = new ActionFireableButtonWidget[summaryInfo.length];
        for (int i = 0; i < summaryInfo.length; i++) {
            //NOTE-- Adds must be in order by column number as defined above
            Vector v = new Vector(5);
            v.add(summaryInfo[i].getEntitlementName());
            v.add(new Integer(summaryInfo[i].getNumMatched()));
            v.add(new Integer(summaryInfo[i].getNumDropped()));
            v.add(new Integer(summaryInfo[i].getNumNew()));
            detailButtons[i] = new ActionFireableButtonWidget("Details"); //$NON-NLS-1$
            v.add(detailButtons[i]);
            detailButtons[i].addActionListener(new DetailButtonActionListener(
                    this, i));
            ((com.metamatrix.toolbox.ui.widget.table.DefaultTableModel)table.getModel()).addRow(v);
        }
    }

    public void detailsRequestedForEntitlement(int subscript) {
        String entName = summaryInfo[subscript].getEntitlementName();
        EntitlementMigrationDetailInfo[] details = null;
        boolean continuing = true;
        try {
            details = infoSource.getDetails(sourceVDB, sourceVDBVersion,
                    targetVDB, targetVDBVersion, entName);
        } catch (Exception ex) {
            continuing = false;
            ExceptionUtility.showMessage("Get Role Migration Details", ex); //$NON-NLS-1$
        }
        if (continuing) {
            final JDialog detailsDialog = new JDialog();
            detailsDialog.setModal(true);
            EntitlementMigrationDetailPanel detPanel =
                    new EntitlementMigrationDetailPanel(details, true);
            detailsDialog.getContentPane().add(detPanel);
            detailsDialog.pack();
            detailsDialog.setTitle("Migration Details for Role " + //$NON-NLS-1$
                    entName);
            detailsDialog.setLocation(StaticUtilities.centerFrame(detailsDialog.getSize()));
            detPanel.getCancelButton().addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    detailsDialog.dispose();
                }
            });
            detailsDialog.show();
        }
    }

    public ActionFireableButtonWidget getDetailButtonAt(int index) {
        return detailButtons[index];
    }
}//end EntitlementMigrationReportSummaryPanel





class DetailButtonActionListener implements ActionListener {
    private int subscript;
    private EntitlementMigrationReportSummaryPanel panel;

    public DetailButtonActionListener(EntitlementMigrationReportSummaryPanel pnl,
            int ss) {
        super();
        panel = pnl;
        subscript = ss;
    }

    public void actionPerformed(ActionEvent ev) {
        panel.detailsRequestedForEntitlement(subscript);
    }
}//end DetailButtonActionListener





class MigrationSummaryTableWidget extends TableWidget implements TableCellRenderer {
    private EntitlementMigrationReportSummaryPanel panel;
    private DefaultTableCellRenderer defaultRenderer = new DefaultTableCellRenderer();

    public MigrationSummaryTableWidget(EntitlementMigrationReportSummaryPanel pnl,
            EnhancedTableModel model) {
        super(model);
        panel = pnl;
        init();
    }

    private void init() {
        this.addMouseListener(new MouseAdapter() {
            public void mouseReleased(MouseEvent ev) {
                Point point = ev.getPoint();
                int row = detailsButtonRow(point);
                if (row >= 0) {
                    ActionFireableButtonWidget button =
                            panel.getDetailButtonAt(row);
                    button.fireActionPerformed(new ActionEvent(this, -1, "")); //$NON-NLS-1$
                }
            }
        });
        this.setRowHeight(30);
    }

    /**
     * If the point is over the "details" column in a displayed row of the
     * table, then return that row number, else -1.
     */
    private int detailsButtonRow(Point point) {
        int row = -1;
        int col = this.columnAtPoint(point);
        if (col == EntitlementMigrationReportSummaryPanel.DETAILS_COLUMN_NUM) {
            row = this.rowAtPoint(point);
        }
        return row;
    }

    public TableCellRenderer getCellRenderer(int row, int column) {
        return this;
    }

    public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
        if (column == EntitlementMigrationReportSummaryPanel.DETAILS_COLUMN_NUM) {
            JButton button = panel.getDetailButtonAt(row);
            JPanel buttonPanel = new JPanel();
            GridBagLayout bl = new GridBagLayout();
            buttonPanel.setLayout(bl);
            buttonPanel.add(button);
            bl.setConstraints(button, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                    GridBagConstraints.CENTER, GridBagConstraints.NONE,
                    new Insets(4, 4, 4, 4), 0, 0));
            return buttonPanel;
        }
        return defaultRenderer.getTableCellRendererComponent(table, value,
                isSelected, hasFocus, row, column);
    }
}//end MigrationSummaryTableWidget

