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

package com.metamatrix.console.ui.dialog;

import java.awt.BorderLayout;
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
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.ui.util.ModifiedDirectoryChooserPanel;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.MessagePanel;
import com.metamatrix.toolbox.ui.widget.TitledBorder;


/** 
 * @since 4.2
 */
public class ErrorDialog extends JDialog {

    /**
     * Maximum number of this dialog to appear on screen.
     * This prevents us from showing lots of dialogs at once in
     * case the server becomes unavailable.
     */
    private static final int MAX_ERRORS_ON_SCREEN = 1;
    
    /**
     * Character used to indent throwables within the excepField ComboBox.
     */
    private static final char THROWABLE_INDENT_CHAR = ' ';
    
    
    /**
     * Number of ErrorDialogs currently displayed
     */
    private static int nErrorsOnScreen = 0;
    private static boolean displayLastException = false;


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
    private boolean detailsPressed = false;
    private ButtonWidget detailsButton = new ButtonWidget("Show Details"); //$NON-NLS-1$
    private ButtonWidget okButton = new ButtonWidget("OK"); //$NON-NLS-1$
    private JPanel upperPanel;
    private JPanel outerPanel;
    private JPanel lowerPanel;
    private String detailsText;
    private JPanel buttonPanel;
    private ButtonWidget saveButton = new ButtonWidget("Save Details to File"); //$NON-NLS-1$
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
    private String initialDCPDirectory = null;
    private boolean successfullySaved = false;
    private String savedFileName = null;

    private java.util.List throwables;
    private java.util.List throwableNames;

    

    public ErrorDialog(Frame owner,
                       String title,
                       String statement,
                       String reason,
                       Throwable exception,
                       String initDCPDir) {
        super(owner, MessagePanel.ERROR_TITLE, true);
        
        this.initialDCPDirectory = initDCPDir;
        JPanel panel = createComponent(title, statement, reason, exception, null, null);
        addToDialog(panel);
        setLocationRelativeTo(owner);
    }


	/**
     * Only show the error window if MAX_ERRORS_ON_SCREEN hasn't been reached.  
	 * @since 4.3
	 */
    public void show() {
        //make sure to display the dialog in the swing thread
        Runnable runnable = new Runnable() {
            public void run() {
                doShow();
            }
        };
        StaticUtilities.invokeLaterSafe(runnable);
        
    }
     
    private void doShow() {
        synchronized (ErrorDialog.class) { 
            if (nErrorsOnScreen < MAX_ERRORS_ON_SCREEN) {
                ++nErrorsOnScreen;
                super.show();
            }
        }
    }
    
    

    private void addToDialog(JPanel panel) {
        getContentPane().setLayout(new BorderLayout());
        getContentPane().add(panel);
    }
    
    public JPanel createComponent(String title,
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
        detailLabel = new LabelWidget("Details"); //$NON-NLS-1$
        detailLabel.setBorder(BorderFactory.createEmptyBorder(15,0,0,0));

        detailArea = new JTextArea();
        detailArea.setLineWrap(true);
        detailArea.setWrapStyleWord(true);
        if ( details != null ) {
            detailArea.setText(convertLineFeeds(details));
            // save off text in case user hits Save To File
            detailsText = "Exception:\n\t" + title + "\n\t\t" + statement + //$NON-NLS-1$ //$NON-NLS-2$
                        "\nreason:\n\t" + reason + "\nMessage:\n\t" + //$NON-NLS-1$ //$NON-NLS-2$
                        detailedMessage + "Details\n\t" + details; //$NON-NLS-1$
            detailsText = convertLineFeeds(detailsText);
        }

        scrollPane = new JScrollPane(detailArea);
        scrollPane.setPreferredSize(new Dimension(180, 100));
        detailArea.setCaretPosition(0);

        if (exception != null){
            throwables = new ArrayList();
            throwableNames = new ArrayList();
            Throwable lastException = parseThrowable(exception, 0);

            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(bas);
            if ( displayLastException ) {
                lastException.printStackTrace(pw);
            } else {
                exception.printStackTrace(pw);
            }
            pw.close();
            detailArea.setText(convertLineFeeds(bas.toString()));


            excepLabel = new LabelWidget("Error:"); //$NON-NLS-1$
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
            msgLabel = new LabelWidget("Message:"); //$NON-NLS-1$

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
            detailsText = "Exception\n\t" + title + "\n\t\t" + statement + //$NON-NLS-1$ //$NON-NLS-2$
                            " \nreason:\n\t" + reason + "\nExceptionType:\n\t" + //$NON-NLS-1$ //$NON-NLS-2$
                            lastException.getClass().getName() + "\nDetails\n\t" + //$NON-NLS-1$
                            convertLineFeeds(bas.toString());
            detailsText = convertLineFeeds(detailsText);

        }
        else {
            if(details !=null){
                 msgLabel = new LabelWidget("Message:  " + detailedMessage); //$NON-NLS-1$
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
        tBorder = new TitledBorder("Reason"); //$NON-NLS-1$
        reasonArea.setName("Reason"); //$NON-NLS-1$
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
        
        
        addWindowListener(new WindowAdapter() {
            private boolean closed = false;

            public void windowClosing(WindowEvent e) {
                doClose();
            }
           

            public void windowClosed(WindowEvent e) {
                doClose();
            }
            
            
            private void doClose() {
                synchronized (ErrorDialog.class) {
                    if (! closed) {
                        closed = true;
                        --nErrorsOnScreen;
                    }
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
        
        return outerPanel;
    }

    private void detailsClicked(){
        if(!detailsPressed){
            detailsPressed = true;
            detailsButton.setText("Hide Details"); //$NON-NLS-1$
            outerPanelLayout.setConstraints(lowerPanel, new GridBagConstraints(0, 1, 1, 1, 1.0,1.0,
                        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

            outerPanel.add(lowerPanel);
            ErrorDialog.this.pack();
            ErrorDialog.this.validate();

        }
        else {
            detailsPressed = false;
            detailsButton.setText("Show Details"); //$NON-NLS-1$
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
        return new StringTokenizer(text, "\n").countTokens(); //$NON-NLS-1$
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
     * Recursively traverse the hierarchy of throwables, and build up the Lists
     * "throwables" and "throwableNames".
     * @since 2.1
     * @param throwable Throwable to traverse.
     * @param depth Current depth - used for indenting display names.
     */
    protected Throwable parseThrowable(final Throwable throwable, int depth) {
        Throwable result = throwable;

        if (throwable == null) {
            return result;
        }
        throwables.add(throwable);
        UniqueThrowableName name = new UniqueThrowableName(throwable.getClass().getName(), depth);
        throwableNames.add(name);
        if (throwable instanceof MetaMatrixException) {
            result = parseThrowable(((MetaMatrixException)throwable).getChild(), depth+1);
        } else if (throwable instanceof MetaMatrixRuntimeException) {
            result = parseThrowable(((MetaMatrixRuntimeException)throwable).getChild(), depth+1);
        } else if (throwable instanceof MultipleException) {
            for (Iterator iter = ((MultipleException)throwable).getExceptions().iterator(); iter.hasNext();) {
                result = parseThrowable((Throwable)iter.next(), depth+1);
            }
        } else if (throwable instanceof InvocationTargetException) {
            result = parseThrowable(((InvocationTargetException)throwable).getTargetException(), depth+1);
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
                String msg = "Error in opening or writing to file."; //$NON-NLS-1$
                JOptionPane.showMessageDialog(this, msg, "File Error", //$NON-NLS-1$
                        JOptionPane.PLAIN_MESSAGE);
            } else {
                FileWriter writer = null;
                try {
                    writer = new FileWriter(file);
                    writer.write(detailsText);
                } catch (Exception ex) {
                    String msg = "Error in opening or writing to file."; //$NON-NLS-1$
                    JOptionPane.showMessageDialog(this, msg, "File Error", //$NON-NLS-1$
                            JOptionPane.PLAIN_MESSAGE);
                }
                if (writer != null) {
                    try {
                        writer.flush();
                        writer.close();
                        successfullySaved = true;
                        savedFileName = sSelectedFileName;
                    } catch (Exception ex) {}
                    
                }

            }
        }
    }
    
    public boolean isSuccessfullySaved() {
        return successfullySaved;
    }
    
    public String getSavedFileName() {
        return savedFileName;
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
        if ((initialDCPDirectory != null) && (initialDCPDirectory.length() > 
                0)) {
            try {
                view.setHome(view.lookup(initialDCPDirectory));
            } catch (Exception ex) {
                //Any exception that may occur in setting the initial view is
                //inconsequential.  This is merely a convenience to the user.
            }
        }
        final FileSystemFilter filter = new FileSystemFilter(view, new String[] {"txt"}, "Text documents (*.txt)"); //$NON-NLS-1$ //$NON-NLS-2$
        final ModifiedDirectoryChooserPanel dirPanel =
                new ModifiedDirectoryChooserPanel(view,
                                          DirectoryChooserPanel.TYPE_SAVE,
                                          new FileSystemFilter[] {filter} );
                                          
        DialogWindow.show(this, "Save Details", dirPanel); //$NON-NLS-1$
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
    
    
    
    /**
     * Simple dataholder for ThrowableNames, for use in the excepField ComboBox.
     * We can't just use string, because two different strings will be equal(),
     * which messes up the ComboBox behavior.  
     * Also includes spaces to indicate the hierarchy of throwables.
     */
    private class UniqueThrowableName {
        public String name;
        
        /**
         * @param name
         * @param depth
         * @since 4.3
         */
        public UniqueThrowableName(String name, int depth) {
            StringBuffer buffer = new StringBuffer();
            for (int i=0; i<depth; i++) {
                buffer.append(THROWABLE_INDENT_CHAR);
            }
            buffer.append(name);
            this.name = buffer.toString();
        }
        
        public String toString() {
            return name;
        }
    }
}
