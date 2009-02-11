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
package com.metamatrix.toolbox.ui.widget;

import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import javax.swing.*;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.MultipleRuntimeException;

import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * @since 2.0
 * @version 2.1
 */
public class ErrorDialog extends JDialog {

//    private static String sDirectoryName = null;
    private static boolean displayLastException = false;

//    private static final int MAX_EXCEPTION_MSG_LENGTH = 200;
//    private static final int MAX_EXCEPTION_TYPE_LENGTH = 35;
//    private static final String OUT_LOG_FILE_PROP_NAME = "metamatrix.stdout.file";

    /**
     * sets a static property that controls how ErrorDialogs generated henceforth will
     * default to showing either the first or last exception in a chain of exceptions.
     * The default is to show the first.
     * @param showLast if true, ErrorDialog will initialize to show the last exception
     * in a chain.
     */
    public static void setDisplayLastException(boolean showLast) {
        displayLastException = showLast;
    }

    private final int ERROR_DIALOG_WIDTH = 400;
    private final int ERROR_DIALOG_HEIGHT_H = 150;
    private final int WHOLE_DIALOG_HEIGHT_H = 500;
    private int WHOLE_DIALOG_HEIGHT_W;
//    private final int LINE_BREAK_MEASURE = 270;
    private boolean detailsPressed = false;
    private ButtonWidget detailsButton = new ButtonWidget("Show Details");
    private ButtonWidget okButton = new ButtonWidget("OK");
//    private Hashtable severityTable = new Hashtable(4);
//    private JScrollPane jScrollPane;
    private JPanel upperPanel;
    private JPanel outerPanel;
    private JPanel lowerPanel;
    private String detailsText;
    private JPanel buttonPanel;
//    private JPanel bPositionPanel;
    private ButtonWidget saveButton = new ButtonWidget("Save Details to File");
//    private Dimension minimumSize = new Dimension(300, 180);
    private LabelWidget msgLabel;
    private JTextArea msgArea ;
    private JPanel linePanel;
    private JPanel excMsgPanel;
    private GridBagLayout excMsgLayout;
    private int rowCount;
    private LabelWidget detailLabel;
    private JTextArea detailArea;
    private JScrollPane scrollPane;
    private Font font;
    private LabelWidget excepLabel;
    private JComboBox excepField;
    private GridBagLayout lowerPanelLayout;
    private GridBagLayout outerPanelLayout;
    private LabelWidget statementArea;
    private JPanel stateReasonPanel;
    private JTextArea  reasonArea;
    private JPanel outterButtonPanel;
    private boolean savesLastFileLoc;

    private List throwables;
    private List throwableNames;

    public ErrorDialog(Frame owner,
                       String title,
                       String statement,
                       String reason)
    {
        super(owner, MessagePanel.ERROR_TITLE, true);
        detailsButton.setEnabled(false);
        createComponent(title, statement, reason, null, null, null);
        setLocationRelativeTo(owner);
    }

    public ErrorDialog(Frame owner,
                       String title,
                       String statement,
                       String reason,
                       Throwable exception)
    {
        super(owner, MessagePanel.ERROR_TITLE, true);
        createComponent(title, statement, reason, exception,
                        null, null);
        setLocationRelativeTo(owner);
    }

    public ErrorDialog(Frame owner,
                       String title,
                       String statement,
                       String reason,
                       String detailedMessage,
                       String details)
    {
        super(owner, MessagePanel.ERROR_TITLE, true);
        createComponent(title, statement, reason, null, detailedMessage,
                        details);
        setLocationRelativeTo(owner);
    }

    public ErrorDialog(Dialog owner,
                       String title,
                       String statement,
                       String reason)
    {
        super(owner, MessagePanel.ERROR_TITLE, true);
        detailsButton.setEnabled(false);
        createComponent(title, statement, reason, null, null, null);
        setLocationRelativeTo(owner);
    }

    public ErrorDialog(Dialog owner,
                       String title,
                       String statement,
                       String reason,
                       Throwable exception)
    {
        super(owner, MessagePanel.ERROR_TITLE, true);
        createComponent(title, statement, reason, exception,
                        null, null);
        setLocationRelativeTo(owner);
    }

    public ErrorDialog(Dialog owner,
                       String title,
                       String statement,
                       String reason,
                       String detailedMessage,
                       String details)
    {
        super(owner, MessagePanel.ERROR_TITLE, true);
        createComponent(title, statement, reason, null, detailedMessage,
                        details);
        setLocationRelativeTo(owner);
    }

    private void createComponent(String title,
                                String statement,
                                String reason,
                                final Throwable exception,
                                String detailedMessage,
                                String details)
    {
        outerPanel = new JPanel();
        Dimension edDimension = Toolkit.getDefaultToolkit().getScreenSize();
        Double edDimensionW = new Double (edDimension.getWidth()*3/4);
        
        WHOLE_DIALOG_HEIGHT_W = edDimensionW.intValue();

        outerPanelLayout = new GridBagLayout();
        outerPanel.setLayout(outerPanelLayout);
        getContentPane().setLayout(new BorderLayout());
        upperPanel = new JPanel();
        upperPanel.setLayout(new BoxLayout(upperPanel,BoxLayout.Y_AXIS));

        //lowerPanel consists of exception, exception message, detail about exception ,save .
        // If detail button is clicked once, the lowPanel is shown

        lowerPanel = new JPanel();
        lowerPanelLayout = new GridBagLayout();
        lowerPanel.setLayout(lowerPanelLayout);
        excMsgPanel = new JPanel();
        excMsgLayout = new GridBagLayout();
        excMsgPanel.setLayout(excMsgLayout);
        detailLabel = new LabelWidget("Details");
        detailLabel.setBorder(BorderFactory.createEmptyBorder(15,0,0,0));

        detailArea = new JTextArea();
        detailArea.setLineWrap(true);
        detailArea.setWrapStyleWord(true);
        if ( details != null ) {
            detailArea.setText(convertLineFeeds(details));
            // save off text in case user hits Save To File
            detailsText = "Exception:\n\t" + title + "\n\t\t" + statement +
                        "\nreason:\n\t" + reason + "\nMessage:\n\t" +
                        detailedMessage + "Details\n\t" + details;
            detailsText = convertLineFeeds(detailsText);
        }

        scrollPane = new JScrollPane(detailArea);
        scrollPane.setPreferredSize(new Dimension(180, 100));
        detailArea.setCaretPosition(0);

        if (exception != null){
        	throwables = new ArrayList();
        	throwableNames = new ArrayList();
            Throwable lastException = parseThrowable(exception);

            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(bas);
            if ( displayLastException ) {
                lastException.printStackTrace(pw);
            } else {
                exception.printStackTrace(pw);
            }
            pw.close();
            detailArea.setText(convertLineFeeds(bas.toString()));


            excepLabel = new LabelWidget("Error:");
            excepField = new JComboBox(throwableNames.toArray());
            msgArea = new JTextArea();
            if ( displayLastException ) {
                msgArea.setText(parseMessage(lastException.getMessage()));
            } else {
                msgArea.setText(parseMessage(exception.getMessage()));
            }
            excepField.addItemListener(new ItemListener(){
                public void itemStateChanged(ItemEvent e){
                    final int ndx = excepField.getSelectedIndex();
                    if (ndx < 0) {
                        return;
                    }
                    final Throwable throwable = (Throwable)throwables.get(ndx);
                    msgArea.setText(parseMessage(throwable.getMessage()));
                    final ByteArrayOutputStream bas = new ByteArrayOutputStream();
                    final PrintWriter pw = new PrintWriter(bas);
                    throwable.printStackTrace(pw);
                    pw.close();
                    detailArea.setText(convertLineFeeds(bas.toString()));
                    detailArea.setCaretPosition(0);
                }
            });
            if ( displayLastException ) {
                excepField.setSelectedIndex(throwableNames.size() - 1);
            }

            excepLabel.setLabelFor(excepField);
            msgLabel = new LabelWidget("Message:");

            msgArea.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createEtchedBorder(),
                    		  BorderFactory.createEmptyBorder(2,0,2,0)));
            msgArea.setBackground(excMsgPanel.getBackground());
            msgArea.setLineWrap(true);
            msgArea.setWrapStyleWord(true);
            msgArea.setEditable(false);
            msgLabel.setLabelFor(msgArea);
            final JScrollPane msgScroller = new JScrollPane(msgArea);
            excMsgLayout.setConstraints(excepLabel, new GridBagConstraints(0, 0, 1, 1, 0, 0,
                                        GridBagConstraints.NORTHEAST, GridBagConstraints.NONE, new Insets(1, 1, 5, 5), 0, 0));

            excMsgLayout.setConstraints(excepField, new GridBagConstraints(1, 0, 1, 1, 0, 1.0,
                                        GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(1, 1, 5, 1), 0, 0));

            excMsgLayout.setConstraints(msgLabel , new GridBagConstraints(0, 1, 1, 1, 0 , 0,
                                     GridBagConstraints.NORTHEAST, GridBagConstraints.NONE, new Insets(1, 1, 1, 5), 0, 0));

            excMsgLayout.setConstraints(msgScroller, new GridBagConstraints(1, 1, 1, 1, 1, 1,
                                     GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(1, 1, 1, 1), 0, 0));

            excMsgPanel.add(excepLabel);
            excMsgPanel.add(excepField);
            excMsgPanel.add(msgLabel);
            excMsgPanel.add(msgScroller);
            lowerPanel.add(excMsgPanel);

            // save off exception message in case user hits Save To File
            bas = new ByteArrayOutputStream();
            pw = new PrintWriter(bas);
            Iterator iter = throwables.iterator();
            while ( iter.hasNext() ) {
                Throwable t = (Throwable) iter.next();
                String message = parseMessage(t.getMessage());
                if ( message != null ) {
                    pw.write(message);
                }
                t.printStackTrace(pw);
                pw.write('\n');
            }
            pw.flush();
            detailsText = "Exception\n\t" + title + "\n\t\t" + statement +
                            " \nreason:\n\t" + reason + "\nExceptionType:\n\t" +
                            lastException.getClass().getName() + "\nDetails\n\t" +
                            convertLineFeeds(bas.toString());
            detailsText = convertLineFeeds(detailsText);

        }
        else {
            if(details !=null){
                 msgLabel = new LabelWidget("Message:  " + detailedMessage);
                 excMsgLayout.setConstraints(msgLabel, new GridBagConstraints(0, 0, 1, 1, 0, 0,
                                     GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(5, 0, 0, 0), 0, 0));
                 excMsgPanel.add(msgLabel);
                 lowerPanel.add(excMsgPanel);
            }
        }

        linePanel = new JPanel();
        linePanel.setBorder(BorderFactory.createEtchedBorder());
        linePanel.setPreferredSize(new Dimension(100,2));
        lowerPanel.add(linePanel);
        lowerPanel.add(detailLabel);
        lowerPanel.add(scrollPane);

        lowerPanelLayout.setConstraints(linePanel , new GridBagConstraints(0, 0, 1, 1, 0 , 0,
                            GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        lowerPanelLayout.setConstraints(excMsgPanel , new GridBagConstraints(0, 1, 1, 1, 1.0 , 0,
                            GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 0, 0, 0), 0, 0));

        lowerPanelLayout.setConstraints(detailLabel, new GridBagConstraints(0, 2, 1, 1, 0, 0,
                                     GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        lowerPanelLayout.setConstraints(scrollPane, new GridBagConstraints(0, 3, 1, 1, 1.0, 1.0,
                                     GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(1, 1, 5, 1), 5, 5));

        lowerPanelLayout.setConstraints(saveButton, new GridBagConstraints(0, 4, 1, 1, 0, 0,
                                     GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        lowerPanel.add(saveButton);

// following is upperPanel. it consists of stateReasonPanel, outterButtonPanel.

        statementArea = new LabelWidget(statement);
        stateReasonPanel = new JPanel();
        stateReasonPanel.setLayout(new BorderLayout());
        stateReasonPanel.add(statementArea, BorderLayout.NORTH);
        reasonArea = new JTextArea(reason);
        reasonArea.setLineWrap(true);
        reasonArea.setWrapStyleWord(true);
        font = reasonArea.getFont();
        rowCount = getRowCount(reason);
        reasonArea.setBackground(stateReasonPanel.getBackground());
        reasonArea.setEditable(false);
        TitledBorder tBorder;
        tBorder = new TitledBorder("Reason");
        reasonArea.setName("Reason");
        final JScrollPane scroller = new JScrollPane(reasonArea);
        scroller.setBorder(tBorder);
        stateReasonPanel.add(scroller, BorderLayout.CENTER);
        upperPanel.add(stateReasonPanel);

        outterButtonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel = new JPanel();
        buttonPanel.setLayout(new GridLayout(1,2,30,0));
        buttonPanel.add(okButton);
        buttonPanel.add(detailsButton);
        outterButtonPanel.add(buttonPanel);
        upperPanel.add(outterButtonPanel);
        outerPanel.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));
        outerPanel.add(upperPanel);
        outerPanelLayout.setConstraints(upperPanel, new GridBagConstraints(0, 0, 1, 1, 1.0,0.1,
                                    GridBagConstraints.NORTH, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        getContentPane().add(outerPanel);
        this.setSize(ERROR_DIALOG_WIDTH , ERROR_DIALOG_HEIGHT_H+ (int)(1.5*font.getSize()*(rowCount)));
        // set minimumSize for dialog
        addComponentListener(new ComponentAdapter(){
            public void componentResized(ComponentEvent e){
                if (detailsPressed ){
                    if (getSize().width < WHOLE_DIALOG_HEIGHT_W || getSize().height < WHOLE_DIALOG_HEIGHT_H )

                        setSize( WHOLE_DIALOG_HEIGHT_W, WHOLE_DIALOG_HEIGHT_H);
                        setLocationRelativeTo(getOwner());
                    }
                else{
                    if(getSize().width < ERROR_DIALOG_WIDTH || getSize().height < ERROR_DIALOG_HEIGHT_H)
                        setSize(ERROR_DIALOG_WIDTH , ERROR_DIALOG_HEIGHT_H + (int)(1.5*font.getSize()*(rowCount)));
                }
            }
        });

        okButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e){

                okClicked();
            }
        });
        getRootPane().setDefaultButton(okButton);
        detailsButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){

                detailsClicked();
            }
        });
        saveButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                saveClicked();
            }
        });
    }

    private void detailsClicked(){
        if(!detailsPressed){
            detailsPressed = true;
            detailsButton.setText("Hide Details");
            outerPanelLayout.setConstraints(lowerPanel, new GridBagConstraints(0, 1, 1, 1, 1.0,1.0,
                        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

            outerPanel.add(lowerPanel);
            ErrorDialog.this.pack();
            ErrorDialog.this.validate();

        }
        else {
            detailsPressed = false;
            detailsButton.setText("Show Details");
            outerPanel.remove(lowerPanel);
            ErrorDialog.this.validate();
            ErrorDialog.this.pack();
        }
    }

    /**
     * Obtain number of rows needed to display the text in reasonArea in order to adjust the height of the dialog
	 * @since 3.0
	 */
    protected int getRowCount(final String text){
        if (text == null) {
            return 0;
        }
        return new StringTokenizer(text, "\n").countTokens();
    }

    private void okClicked(){
            dispose();
    }

    /**
     * @since 2.1
     */
    protected String parseMessage(final String message) {
        if (message == null) {
            return null;
        }
        final int ndx = message.indexOf('\n');
        if (ndx >= 0) {
            return message.substring(0, ndx);
        }
        return message;
    }

    /**
     * @since 2.1
     */
    protected Throwable parseThrowable(final Throwable throwable) {
        Throwable result = throwable;

        if (throwable == null) {
            return result;
        }
        throwables.add(throwable);
        throwableNames.add(throwable.getClass().getName());
        if (throwable instanceof MetaMatrixException) {
            result = parseThrowable(((MetaMatrixException)throwable).getChild());
        } else if (throwable instanceof MetaMatrixRuntimeException) {
            result = parseThrowable(((MetaMatrixRuntimeException)throwable).getChild());
        } else if (throwable instanceof MultipleException) {
            for (Iterator iter = ((MultipleException)throwable).getExceptions().iterator(); iter.hasNext();) {
            	result = parseThrowable((Throwable)iter.next());
            }
        } else if (throwable instanceof MultipleRuntimeException) {
            for (Iterator iter = ((MultipleRuntimeException)throwable).getThrowables().iterator(); iter.hasNext();) {
            	result = parseThrowable((Throwable)iter.next());
            }
        } else if (throwable instanceof InvocationTargetException) {
            result = parseThrowable(((InvocationTargetException)throwable).getTargetException());
        }
        
        // if one of the chained exceptions did not produce a child exception, then return the input
        if ( result == null ) {
            return result = throwable;
        }
        return result;
    }

    private void saveClicked() {
        //User wants to save the details on a file.  Put up a file chooser.

	    String sSelectedFileName = getFileName();		  	
		if ( sSelectedFileName != null ) {
            File file = new File( sSelectedFileName );
            
            boolean fileOK = false;
            if (file.exists()) {
                //Make sure we can write to file.
                if (file.canWrite()) {
                    fileOK = true;
                }
            } else {
                try {
                    file.createNewFile();
                    fileOK = true;
                } catch (Exception e) {}

            }
            if (!fileOK) {
                String msg = "Error in opening or writing to file.";
                JOptionPane.showMessageDialog(this, msg, "File Error",
                        JOptionPane.PLAIN_MESSAGE);
            } else {
                FileWriter writer = null;
                try {
                    writer = new FileWriter(file);
                    writer.write(detailsText);
                } catch (Exception ex) {
                    String msg = "Error in opening or writing to file.";
                    JOptionPane.showMessageDialog(this, msg, "File Error",
                            JOptionPane.PLAIN_MESSAGE);
                }
                if (writer != null) {
                    try {
                        writer.flush();
                        writer.close();
                    } catch (Exception ex) {}
                    
                }

            }
        }
    }
    

    public static String convertLineFeeds(String str) {

        StringBuffer buf = new StringBuffer();
        char b[] = str.toCharArray();
        char p = '?';

        for ( int i=0; i < b.length; ++i ) {
            if ( (p != '\r' ) && (b[i]=='\n')) buf.append('\r');
            p = b[i];
            buf.append(p);
        }

        return buf.toString();
    }

    
	/**
	 * @since 3.0
	 */
	protected String getFileName() {
        final FileSystemView view = new FileSystemView();
        final FileSystemFilter filter = new FileSystemFilter(view, new String[] {"txt"}, "Text documents (*.txt)");
        final DirectoryChooserPanel dirPanel
            = new DirectoryChooserPanel( view, 
            							  DirectoryChooserPanel.TYPE_SAVE,
            							  new FileSystemFilter[] {filter} );
            							  
        DialogWindow.show(this, "Save Details", dirPanel);
        if (dirPanel.getSelectedButton() != dirPanel.getAcceptButton()) {
            return null;
        }
        final TreeNode node = dirPanel.getSelectedTreeNode();
        if (node == null) {
            return dirPanel.getParentDirectoryEntry().getFullName() + File.separatorChar + dirPanel.getNameFieldText();
        }
        return node.getFullName();
	}
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean savesLastFileLocation() {
        return savesLastFileLoc;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setSavesLastFileLocation(final boolean saves) {
        savesLastFileLoc = saves;
    }

	/////////////////////////////////////////////////////////////////////////////
	/**
	 * @since 3.1
	 */
	public AbstractButton getOKButton() {
		return okButton;
	}

	//############################################################################################################################
    //# Main                                                                                                                     #
    //############################################################################################################################

    /**
     * @since 2.0
     */
    public static void main(final String[] arguments) {
		final MetaMatrixException err = new MetaMatrixException("Really big message line 1, "
                                                                 + "Really big message line 2, "
                                                                 + "Really big message line 3, "
                                                                 + "Really big message line 4, "
                                                                 + "Really big message line 5, "
                                                                 + "Really big message line 6, "
                                                                 + "Really big message line 7, "
                                                                 + "Really big message line 8, "
                                                                 + "Really big message line 9, "
                                                                 + "Really big message line 10, "
                                                                 + "Really big message line 11, "
                                                                 + "Really big message line 12, "
                                                                 + "Really big message line 13, "
                                                                 + "Really big message line 14, "
                                                                 + "Really big message line 15, "
                                                                 + "Really big message line 16, "
                                                                 + "Really big message line 17, "
                                                                 + "Really big message line 18, "
                                                                 + "Really big message line 19, "
                                                                 + "Really big message line 20, "
                                                                 + "Really big message line 21, "
                                                                 + "Really big message line 22, "
                                                                 + "Really big message line 23, "
                                                                 + "Really big message line 24, "
                                                                 + "Really big message line 25, "
                                                                 + "Really big message line 26, "
                                                                 + "Really big message line 27, "
                                                                 + "Really big message line 28, "
                                                                 + "Really big message line 29, "
                                                                 + "Really big message line 30, "
                                                                 + "Really big message line 31, "
                                                                 + "Really big message line 32, "
                                                                 + "Really big message line 33, "
                                                                 + "Really big message line 34, "
                                                                 + "Really big message line 35, "
                                                                 + "Really big message line 36, "
                                                                 + "Really big message line 37, "
                                                                 + "Really big message line 38, "
                                                                 + "Really big message line 39, "
                                                                 + "Really big message line 40, ");
		final MultipleException multErr = new MultipleException();
		multErr.setExceptions(java.util.Arrays.asList(new Throwable[] {new java.awt.AWTError("AWT error"), new NullPointerException(), new RuntimeException("Runtime error")}));
//		err.setChild(multErr);
        final ErrorDialog dlg = new ErrorDialog((Frame)null, "test Dialog", "statement", "1 reason for statement\n" +
        														 "2 reason for statement\n" +
        														 "3 reason for statement\n" +
        														 "4 reason for statement\n" +
        														 "5 reason for statement\n" +
        														 "6 reason for statement\n" +
        														 "7 reason for statement\n" +
        														 "8 reason for statement\n" +
        														 "9 reason for statement\n" +
        														 "10 reason for statement\n" +
        														 "11 reason for statement\n" +
        														 "12 reason for statement\n" +
        														 "13 reason for statement\n" +
        														 "14 reason for statement\n" +
        														 "15 reason for statement\n" +
        														 "16 reason for statement\n" +
        														 "17 reason for statement\n" +
        														 "18 reason for statement\n" +
        														 "19 reason for statement\n" +
        														 "20 reason for statement\n" +
        														 "21 reason for statement\n" +
        														 "22 reason for statement\n",
        														 err);
        dlg.setVisible(true);
        new ErrorDialog((Frame)null, "test Dialog","statement","reason for statement", new NullPointerException()).setVisible(true);
        new ErrorDialog((Frame)null, "test Dialog","statement","reason for statement", "detailed message", "details").setVisible(true);
        new ErrorDialog((Frame)null, "test Dialog","statement","reason for statement", new InvocationTargetException(new IllegalArgumentException("Illegal argument"))).setVisible(true);
        System.exit(0);
    }

}
