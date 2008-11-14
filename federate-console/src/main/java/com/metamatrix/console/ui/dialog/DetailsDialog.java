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

package com.metamatrix.console.ui.dialog;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileWriter;

import javax.swing.*;

import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.util.CenteredOptionPane;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;

public class DetailsDialog extends JDialog {
    public final static int PREFERRED_TEXT_WIDTH = 100;
    public final static int MAX_TEXT_WIDTH =
            StaticUtilities.MAX_MODAL_DIALOG_TEXT_WIDTH;

    private boolean detailsPressed = false;
    private Point smallLocation;
    private LabelWidget iconLabel;
    private JTextArea message;
    private String formattedMessageText;
    private ButtonWidget detailsButton = new ButtonWidget("Details"); //$NON-NLS-1$
    private ButtonWidget okButton = new ButtonWidget("OK"); //$NON-NLS-1$
    private ButtonWidget saveButton = new ButtonWidget("Save Details to File"); //$NON-NLS-1$
    private JTextArea details;
    private JScrollPane jScrollPane;
    private JPanel upperPanel;
    private String detailsText;
    private JPanel buttonPanel;
    private GridBagLayout bl;
    public static final String DIALOG_MESSAGE_TEXT
        = "DetailsDialog.messagetext"; //$NON-NLS-1$
    public static final String DIALOG_DETAILS_TEXT
        = "DetailsDialog.detailstext"; //$NON-NLS-1$

    private Dimension lastDimension;

    public DetailsDialog(Frame parentFrame, String title, String messageText,
                           String dt, Icon icon) {
        super(parentFrame, title ,false);
        setResizable(true);

        iconLabel = new LabelWidget(icon);
        //Insert line breaks (without breaking words) at a reasonable max. length
        formattedMessageText = StaticUtilities.insertLineBreaks(messageText,
                PREFERRED_TEXT_WIDTH, MAX_TEXT_WIDTH);
        message = new JTextArea(formattedMessageText);
        message.setName( DIALOG_MESSAGE_TEXT );

        detailsText = dt;
        details = new JTextArea(detailsText);
        details.setName( DIALOG_DETAILS_TEXT );
        jScrollPane = new JScrollPane(details);

        lastDimension = new Dimension(300,100);
        jScrollPane.setMinimumSize(lastDimension);
        jScrollPane.setPreferredSize(lastDimension);

        okButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                okClicked();
            }
        });
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

        layoutStuff();
        //Change text area to look like a label.
        message.setBackground(upperPanel.getBackground());
        message.setForeground(iconLabel.getForeground());
        message.setFont(iconLabel.getFont());
        pack();
        invalidate();
        setLocation(StaticUtilities.centerFrame(getSize()));
        setResizable(false);

        this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
    }

    private void layoutStuff() {
        getContentPane().setLayout(new BorderLayout());

        upperPanel = new JPanel();
        upperPanel.setLayout(new BoxLayout(upperPanel, BoxLayout.Y_AXIS));

        JPanel labelPanel = new JPanel();
        labelPanel.add(iconLabel);
        labelPanel.add(message);
        upperPanel.add(labelPanel);

        buttonPanel = new JPanel();
        bl = new GridBagLayout();
        buttonPanel.setLayout(bl);
        buttonPanel.add(okButton);
        bl.setConstraints(okButton, new GridBagConstraints(0, 0, 1, 1, 0.1, 0.1, 
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0),
                0, 0));
        buttonPanel.add(detailsButton);
        bl.setConstraints(detailsButton, new GridBagConstraints(1, 0, 1, 1, 0.1, 0.1,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 5, 0, 5),
                0, 0));
        buttonPanel.add(saveButton);
        bl.setConstraints(saveButton, new GridBagConstraints(2, 0, 1, 1, 0.1, 0.1,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 0, 0, 0),
                0, 0));
        saveButton.setVisible(false);
        upperPanel.add(buttonPanel);

        getContentPane().add(upperPanel, BorderLayout.NORTH);
    }

    private void okClicked(){
        dispose();
    }

    private void detailsClicked(){
        if (!detailsPressed){
            detailsPressed = true;
            saveButton.setVisible(true);
            getContentPane().add(jScrollPane, BorderLayout.CENTER);
            smallLocation = this.getLocation();
            this.getSize();
            if ( ViewManager.getMainFrame() != null ) {
                Point thisLocation = ViewManager.getMainFrame().getLocation();
                Dimension thisSize = ViewManager.getMainFrame().getSize();
                this.setLocation(thisLocation);
                this.setSize(thisSize);
            } else {
                this.setLocation(0,0);
                this.setSize(Toolkit.getDefaultToolkit().getScreenSize());
            }
        }else{
            detailsPressed = false;
            lastDimension = jScrollPane.getSize();
//            Dimension thisSize = this.getSize();
            getContentPane().remove(jScrollPane);
            saveButton.setVisible(false);
            this.setLocation(smallLocation);
            pack();
        }
        setResizable(detailsPressed);
        super.validate();
    }

    private void saveClicked() {
        //User wants to save the details on a file.  Put up a file chooser.
        DetailsFileChooser chooser = new DetailsFileChooser();
        int response = chooser.showSaveDialog(null);
        if (response == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
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
                } catch (Exception e) {
                }
            }
            if (!fileOK) {
                String msg = "Error in opening or writing to file."; //$NON-NLS-1$
                CenteredOptionPane.showMessageDialog(
                        ViewManager.getMainFrame(), msg, "File Error", //$NON-NLS-1$
                        JOptionPane.PLAIN_MESSAGE, null);
            } else {
                FileWriter writer = null;
                try {
                    writer = new FileWriter(file);
                    writer.write(detailsText);
                } catch (Exception ex) {
                    String msg = "Error in opening or writing to file."; //$NON-NLS-1$
                    CenteredOptionPane.showMessageDialog(
                            ViewManager.getMainFrame(), msg, "File Error", //$NON-NLS-1$
                            JOptionPane.PLAIN_MESSAGE, null);
                }
                if (writer != null) {
                    try {
                        writer.flush();
                        writer.close();
                    } catch (Exception ex) {
                    }
                }
            }
        }
    }
}//end DetailsDialog



class DetailsFileChooser extends JFileChooser {
    public DetailsFileChooser() {
        super(new File(System.getProperty("user.dir"))); //$NON-NLS-1$
        setFileSelectionMode(JFileChooser.FILES_ONLY);
    }
} //end DetailsFileChooser
