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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.util.Iterator;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class VdbWizardEntitlementsPanel extends BasicWizardSubpanelContainer implements
                                                                            ComponentListener {

    // data 
    String sVdbName = "";
    String sDescription = "";

    // used when this panel is doing a new version
    VirtualDatabase vdbSourceVdb = null;
    VirtualDatabase vdbNewVdb = null;

    private EntitlementMigrationReport emrEntitlementReport = null;
    GridLayout gridLayout1 = new GridLayout();
    JPanel pnlOuter = new JPanel();
    GridBagLayout gridBagLayout1 = new GridBagLayout();
    private VdbConnBindPanel pnlConnectorBinding = null;
    private JEditorPane pnlEditor = null;
    private javax.swing.JTabbedPane tpnVdbStuff = new JTabbedPane();
    private JPanel pnlReportOuter = new JPanel();
    private ButtonWidget btnWriteReportToFile = new ButtonWidget("Save to File");
    private JPanel pnlButtons = new JPanel();
    JTextArea txaDescription = new JTextArea();

    JLabel lblDescription = new JLabel();
    TextFieldWidget txfName = new TextFieldWidget();
    JLabel lblName = new JLabel();
    JScrollPane jScrollPane2 = new JScrollPane();

    private ConnectionInfo connection;
    private int iLimit = 200;

    public VdbWizardEntitlementsPanel(VirtualDatabase vdb,
                                      WizardInterface wizardInterface,
                                      ConnectionInfo connection,
                                      int stepNum) {
        super(wizardInterface);
        this.connection = connection;
        setSourceVdb(vdb);
        try {
            jbInit(stepNum);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        setSourceVdb(vdb);

        if(getSourceVdb()!=null) {
        	setVdbName(getSourceVdb().getName());
        	txfName.setText(getSourceVdb().getName());
        	setDescription(getSourceVdb().getDescription());
        	txaDescription.setText(getSourceVdb().getDescription());
        }
        txfName.setEditable(false);
        txaDescription.setLineWrap(true);
        txaDescription.setWrapStyleWord(true);

    }

    private VirtualDatabase getSourceVdb() {
        return vdbSourceVdb;
    }

    private void setSourceVdb(VirtualDatabase vdb) {
        vdbSourceVdb = vdb;
    }

    public void setNewVdb(VirtualDatabase vdb) {
        vdbNewVdb = vdb;
        pnlConnectorBinding.setVirtualDatabase(getNewVdb());
    }

    private VirtualDatabase getNewVdb() {
        return vdbNewVdb;
    }

    public void setEntitlementMigrationReport(EntitlementMigrationReport emrEntitlementReport) {
        String sReport = "";
        this.emrEntitlementReport = emrEntitlementReport;
        
        if ( getEntitlementMigrationReport().getEntries().size() > iLimit ) {
            sReport = createEntitlementMigrationStatsOnlyOutputAsString(getEntitlementMigrationReport());            
        } else {
            sReport = createEntitlementMigrationFullReportOutputAsString(getEntitlementMigrationReport());
        }

        pnlEditor.setText(sReport);
    }

    public EntitlementMigrationReport getEntitlementMigrationReport() {
        if (emrEntitlementReport != null) {
            return emrEntitlementReport;
        }
        return getEmptyMigrationReport();
    }

    public String getVdbName() {
        getDataFromPanel();
        return sVdbName;
    }

    public String getDescription() {
        getDataFromPanel();
        return sDescription;
    }

    public void setVdbName(String sVdbName) {
        this.sVdbName = sVdbName;
    }

    public void setDescription(String sDesc) {
        this.sDescription = sDesc;
    }

    public void getDataFromPanel() {
        setVdbName(txfName.getText());
        setDescription(txaDescription.getText());
    }

    public void putDataIntoPanel() {
        txfName.setText(getVdbName());
        txaDescription.setText(getDescription());
    }

    public void setupListening() {
        txfName.getDocument().addDocumentListener(new DocumentListener() {

            public void changedUpdate(DocumentEvent de) {
                resolveForwardButton();
            }

            public void insertUpdate(DocumentEvent de) {
                resolveForwardButton();
            }

            public void removeUpdate(DocumentEvent de) {
                resolveForwardButton();
            }
        });
        addComponentListener(this);
    }

    // methods required by ComponentListener Interface

    public void componentMoved(ComponentEvent e) {
        // setInitialPostRealizeState();
    }

    public void componentResized(ComponentEvent e) {
        setInitialPostRealizeState();
        removeComponentListener(this);
    }

    public void componentShown(ComponentEvent e) {
        // setInitialPostRealizeState();
    }

    public void componentHidden(ComponentEvent e) {
        // setInitialPostRealizeState();
    }

    public void setInitialPostRealizeState() {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(true);
        txfName.requestFocus();
    }

    public void resolveForwardButton() {
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        if (txfName.getText().trim().equals("")) {
            forwardButton.setEnabled(false);
        } else {
            forwardButton.setEnabled(true);
        }
    }

    public void postRealize() {
        txfName.requestFocus();
        AbstractButton forwardButton = getWizardInterface().getForwardButton();
        forwardButton.setEnabled(true);
    }

    private void jbInit(int stepNum) throws Exception {

        createPanel();

        setupListening();

        setMainContent(pnlOuter);

        setStepText(stepNum, "View Roles and/or Connector Binding Migration Results");
    }

    private void createPanel() {
        pnlConnectorBinding = new VdbConnBindPanel(connection);
        pnlConnectorBinding.getEditButton().setVisible(false);

        pnlEditor = new JEditorPane();
        pnlEditor.setContentType("text/html");
        pnlEditor.setEditable(false);

        JScrollPane scpReport = new JScrollPane();
        pnlReportOuter.setLayout(new BorderLayout());
        pnlReportOuter.add(scpReport, BorderLayout.CENTER);

        pnlButtons.add(btnWriteReportToFile);

        pnlReportOuter.add(pnlButtons, BorderLayout.SOUTH);

        btnWriteReportToFile.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent event) {
                writeToFile();
            }
        });
        Dimension dimButtonSize = btnWriteReportToFile.getPreferredSize();

        btnWriteReportToFile.setPreferredSize(new Dimension(100, dimButtonSize.height));

        tpnVdbStuff.addTab("Role Migration Report", pnlReportOuter);

        scpReport.setViewportView(pnlEditor);
        scpReport.setPreferredSize(new Dimension(600, 350));

        tpnVdbStuff.addTab("Connector Bindings", pnlConnectorBinding);

        gridLayout1.setVgap(7);
        gridLayout1.setRows(2);
        gridLayout1.setColumns(1);
        gridLayout1.setColumns(1);
        pnlOuter.setLayout(new BorderLayout());
        pnlOuter.add(tpnVdbStuff, BorderLayout.CENTER);
    }

    // ==========================================

    private EntitlementMigrationReport getEmptyMigrationReport() {
        String sourceNameVers = null;
        VirtualDatabase srcVdb = getSourceVdb();
        if(srcVdb!=null) {
        	VirtualDatabaseID srcVdbID = (VirtualDatabaseID)srcVdb.getID();
        	sourceNameVers = srcVdbID.getName();
        } else {
        	sourceNameVers = "source"; //$NON-NLS-1$
        }
        
        String targetNameVers = null;
        VirtualDatabase tgtVdb = getNewVdb();
        if(tgtVdb!=null) {
        	VirtualDatabaseID tgtVdbID = (VirtualDatabaseID)tgtVdb.getID();
        	targetNameVers = tgtVdbID.getName()+" Version "+tgtVdbID.getVersion();
        } else {
        	targetNameVers = "targetVDB"; //$NON-NLS-1$
        }
        
        EntitlementMigrationReport emptyReport = new EntitlementMigrationReport(sourceNameVers, targetNameVers); 

        return emptyReport;
    }

    private String createEntitlementMigrationStatsOnlyOutputAsString(EntitlementMigrationReport emr) {

        String SUCCEEDED = "Succeeded";
        String sMigratedStatus = "";
        int iSucceeded = 0;
        int iTotalEntries = emr.getEntries().size();
        Iterator it = emr.getEntries().iterator();
        while (it.hasNext()) {
            List lstEntry = (List)it.next();
        
            sMigratedStatus = (String)lstEntry.get(EntitlementMigrationReport.MIGRATED_INDEX);
            if ( sMigratedStatus.trim().equals( SUCCEEDED ) ) {
                iSucceeded++;    
            }
        }
        
        StringBuffer sbReport = new StringBuffer ( 128 );

        String sTitleFontSizeStandalone = "<FONT size= +1>";
        String sSubtitleFontSizeStandalone = "<FONT size= +0>";
        String sSmallFontSizeStandalone = "<FONT size= -1>";
        String sEndFont = "</FONT>";

        StringBuffer sbHead = new StringBuffer ("<html> <head> </head> <body>");

        sbHead.append( "<DIV ALIGN=CENTER>" );
        sbHead.append(  sTitleFontSizeStandalone );
        sbHead.append( "Roles Migration Report<br>" );
        sbHead.append( sEndFont );
        sbHead.append( sSubtitleFontSizeStandalone );
        sbHead.append( "Source VDB: " );
        sbHead.append( emr.getSourceVDBID() );
        sbHead.append( "&nbsp&nbsp&nbsp&nbsp" );
        sbHead.append( " Target VDB: " );
        sbHead.append( emr.getTargetVDBID() );
        sbHead.append( "<br>" );

        sbHead.append( " <table> " );
        sbHead.append( "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Succeeded" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        sbHead.append(  "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Failed" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        sbHead.append( "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Total" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );

        // Stats row
        StringBuffer sbRow = new StringBuffer ();        
        sbRow.append( "<tr>" );    

        sbRow.append( "<td " );    
        sbRow.append( "ALIGN='CENTER'" );    
        sbRow.append( " >" );    
        sbRow.append( sSmallFontSizeStandalone );    
        sbRow.append( iSucceeded );    
        sbRow.append( sEndFont );    
        sbRow.append( "</td>" );    
        
        sbRow.append( "<td " );    
        sbRow.append( "ALIGN='CENTER'" );    
        sbRow.append( " >" );    
        sbRow.append( sSmallFontSizeStandalone );    
        sbRow.append( iTotalEntries - iSucceeded );    
        sbRow.append( sEndFont );    
        sbRow.append( "</td>" );    
        
        sbRow.append( "<td " );    
        sbRow.append( "ALIGN='CENTER'" );    
        sbRow.append( " >" );    
        sbRow.append( sSmallFontSizeStandalone );    
        sbRow.append( iTotalEntries );    
        sbRow.append( sEndFont );    
        sbRow.append( "</td>" );    
        
        sbRow.append( "</tr>" );
        sbHead.append( sbRow );
        
        // Message row
        sbRow.delete(0, sbRow.length() );
        sbRow.append( "<tr>" );    

        sbRow.append( "<td colspan='3'" );    
        sbRow.append( "ALIGN='CENTER'" );    
        sbRow.append( " >" );    
        sbRow.append( sSmallFontSizeStandalone );    
        sbRow.append( "<i>To see details, click Save to File and review the file.</i>" );    
        sbRow.append( sEndFont );    
        sbRow.append( "</td>" );    
        sbRow.append( "</tr>" );

        sbHead.append( sbRow );
        
        sbHead.append( " </table> " );
                
        sbReport.append( sbHead );        

        StringBuffer sbTail = new StringBuffer ( "</body> </html>" );
        sbReport.append( sbTail );
        
        return sbReport.toString();
    }

    private String createEntitlementMigrationFullReportOutputAsString(EntitlementMigrationReport emr) {

        StringBuffer sbReport = new StringBuffer ( 1024 * 4 );

        String sTitleFontSizeStandalone = "<FONT size= +1>";
        String sSubtitleFontSizeStandalone = "<FONT size= +0>";
        String sSmallFontSizeStandalone = "<FONT size= -1>";
        String sEndFont = "</FONT>";

        // <DIV ALIGN=LEFT|RIGHT|CENTER|JUSTIFY></DIV>
        StringBuffer sbHead = new StringBuffer ("<html> <head> </head> <body>");

        sbHead.append( "<DIV ALIGN=CENTER>" );
        sbHead.append(  sTitleFontSizeStandalone );
        sbHead.append( "Roles Migration Report<br>" );
        sbHead.append( sEndFont );
        sbHead.append( sSubtitleFontSizeStandalone );
        sbHead.append( "Source VDB: " );
        sbHead.append( emr.getSourceVDBID() );
        sbHead.append( "&nbsp&nbsp&nbsp&nbsp" );
        sbHead.append( " Target VDB: " );
        sbHead.append( emr.getTargetVDBID() );
        sbHead.append( sEndFont );
        sbHead.append( "</DIV>" );
        
        // Table headings
        sbHead.append( " <table> " );
        sbHead.append( "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Migrated" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        sbHead.append(  "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Resource" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        sbHead.append( "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Source Policy ID" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        sbHead.append( "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Target Policy ID" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        sbHead.append( "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Actions" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        sbHead.append( "<th>" );
        sbHead.append( sSmallFontSizeStandalone );
        sbHead.append( "Reason" );
        sbHead.append( sEndFont );
        sbHead.append( "</th>" );
        
        StringBuffer sbTail = new StringBuffer ( "</table>" );
        sbTail.append( sEndFont );
        sbTail.append( " + </body> </html>" );

        sbReport.append( sbHead );        

        // Table rows
        StringBuffer sbRow = new StringBuffer ( 1024 );
        Iterator it = emr.getEntries().iterator();
        while (it.hasNext()) {

            // reinitialize the sbRow stringbuffer
            sbRow.delete(0, sbRow.length() );

            List lstEntry = (List)it.next();
            sbRow.append( "<tr " + " >" );    

            sbRow.append( "<td " );    
            sbRow.append( "ALIGN='CENTER'" );    
            sbRow.append( " >" );    
            sbRow.append( sSmallFontSizeStandalone );    
            sbRow.append( lstEntry.get(EntitlementMigrationReport.MIGRATED_INDEX) );    
            sbRow.append( sEndFont );    
            sbRow.append( "</td>" );    
            
            sbRow.append( "<td " );    
            sbRow.append( "ALIGN='LEFT'" );    
            sbRow.append( " >" );
            sbRow.append( sSmallFontSizeStandalone );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.RESOURCE_INDEX) );
            sbRow.append( sEndFont );
            sbRow.append( "</td>" );  //$NON-NLS-1$
            
            sbRow.append( "<td " );
            sbRow.append( "ALIGN='CENTER'" );
            sbRow.append( " >" );
            sbRow.append( sSmallFontSizeStandalone );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.SOURCE_POLICYID_INDEX) );
            sbRow.append( sEndFont );
            sbRow.append( "</td>" );
            
            sbRow.append( "<td " );
            sbRow.append( "ALIGN='CENTER'" );
            sbRow.append( " >" );
            sbRow.append( sSmallFontSizeStandalone );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.TARGET_POLICYID_INDEX) );
            sbRow.append( sEndFont );
            sbRow.append( "</td>" );
            
            sbRow.append( "<td " );
            sbRow.append( "ALIGN='LEFT'" );
            sbRow.append( " >" );
            sbRow.append( sSmallFontSizeStandalone );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.ACTIONS_INDEX) );
            sbRow.append( sEndFont );
            sbRow.append( "</td>" );
            
            sbRow.append( "<td " );
            sbRow.append( "ALIGN='LEFT'" );
            sbRow.append( " >" );
            sbRow.append( sSmallFontSizeStandalone );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.REASON_INDEX) );
            sbRow.append( sEndFont );
            sbRow.append( "</td>" );
            sbRow.append( "</tr>" );
            
            sbReport.append( sbRow );
        }

        sbReport.append( sbTail );
        
        return sbReport.toString();
    }

    // / * =====================================================================
    // Code sample from Dan: writing 'table' data to a text file

    private void writeToFile() {

        //        String TAB_CHAR = "\t";
        //        String sRow = "";

        DirectoryChooserPanel pnlChooser = new DirectoryChooserPanel(new FileSystemView(), DirectoryChooserPanel.TYPE_SAVE);
        pnlChooser.setAcceptButtonLabel("Save");

        DialogWindow.show(this, "Save Roles Migration Report", pnlChooser);
        if (pnlChooser.getSelectedButton() == pnlChooser.getAcceptButton()) {
            DirectoryEntry result = (DirectoryEntry)pnlChooser.getSelectedTreeNode();

            try {
                getEntitlementMigrationReport().writeReport(result.getNamespace());
            } catch (Exception theException) {
                theException.printStackTrace();
                ExceptionUtility.showMessage(theException.getMessage(), theException);
                LogManager.logError(LogContexts.SYSTEMLOGGING, theException, getClass() + ":writeFile");
            }
            // the chooser panel handles if the file can be written to
            // or if it can be created
            //            String LINE_SEP = System.getProperty("line.separator");
            //            StringBuffer txt = new StringBuffer();
            //
            //            // append column headers
            //            String sHeaders
            //                = "Migrated" + TAB_CHAR
            //                + "Resource" + TAB_CHAR
            //                + "Source Policy ID" + TAB_CHAR
            //                + "Target Policy ID" + TAB_CHAR
            //                + "Actions" + TAB_CHAR
            //                + "Reason";
            //
            //            txt.append(sHeaders);
            //            txt.append(LINE_SEP);
            //
            //            // append data
            //
            //            Iterator it
            //                = getEntitlementMigrationReport().getEntries().iterator();
            //
            //            while (it.hasNext()) {
            //                sRow = "";
            //                List lstEntry = (List)it.next();
            //
            //                sRow += lstEntry.get(EntitlementMigrationReport.MIGRATED_INDEX)
            //                        + TAB_CHAR;
            //                sRow += lstEntry.get(EntitlementMigrationReport.RESOURCE_INDEX)
            //                        + TAB_CHAR;
            //                sRow += lstEntry.get(EntitlementMigrationReport.SOURCE_POLICYID_INDEX)
            //                        + TAB_CHAR;
            //                sRow += lstEntry.get(EntitlementMigrationReport.TARGET_POLICYID_INDEX)
            //                        + TAB_CHAR;
            //                sRow += lstEntry.get(EntitlementMigrationReport.ACTIONS_INDEX)
            //                        + TAB_CHAR;
            //                sRow += lstEntry.get(EntitlementMigrationReport.REASON_INDEX);
            //
            //                txt.append(sRow);
            //                txt.append(LINE_SEP);
            //            }
            //
            //
            //            // write the file
            //            try {
            //                FileWriter writer = new FileWriter(result.getNamespace());
            //                writer.write(txt.toString());
            //                writer.flush();
            //                writer.close();
            //            } catch (Exception theException) {
            //                theException.printStackTrace();
            //                ExceptionUtility.showMessage(theException.getMessage(),
            //                                             theException);
            //                LogManager.logError(LogContexts.SYSTEMLOGGING,
            //                                    theException,
            //                                    getClass() + ":writeFile");
            //            }
        }
    }
}
