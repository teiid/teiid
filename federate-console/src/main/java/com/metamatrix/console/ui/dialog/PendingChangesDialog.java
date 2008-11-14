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
import java.awt.event.*;
import javax.swing.*;

import com.metamatrix.console.util.*;
import com.metamatrix.toolbox.ui.widget.*;

public class PendingChangesDialog extends JDialog {
	public static final int YES = DialogUtility.YES;
    public static final int NO = DialogUtility.NO;
    public static final int CANCEL = DialogUtility.CANCEL;
    
    private AbstractButton yesButton;
    private AbstractButton noButton;
    private AbstractButton cancelButton;
    private int response = -1;
    
    public PendingChangesDialog(JFrame owner, String msg, String url,
    		String userName) {
    	super(owner, "Changes Pending", true); //$NON-NLS-1$
    	init(msg, url, userName);
    }
    
    private void init(String msg, String url, String userName) {
    	this.addWindowListener(new WindowAdapter() {
    		public void windowClosing(WindowEvent ev) {
    			cancelPressed();
    		}
    	});
    	
    	yesButton = new ButtonWidget("Yes"); //$NON-NLS-1$
    	noButton = new ButtonWidget("No"); //$NON-NLS-1$
    	cancelButton = new ButtonWidget("Cancel"); //$NON-NLS-1$
    	yesButton.addActionListener(new ActionListener() {
    		public void actionPerformed(ActionEvent ev) {
    			yesPressed();
    		}
    	});
    	noButton.addActionListener(new ActionListener() {
    		public void actionPerformed(ActionEvent ev) {
    			noPressed();
    		}
    	});
    	cancelButton.addActionListener(new ActionListener() {
    		public void actionPerformed(ActionEvent ev) {
    			cancelPressed();
    		}
    	});
    	JPanel buttonsPanel = new JPanel(new GridLayout(1, 3, 10, 0));
    	buttonsPanel.add(yesButton);
    	buttonsPanel.add(noButton);
    	buttonsPanel.add(cancelButton);
    	
    	String formattedString = StaticUtilities.insertLineBreaks(msg,
                StaticUtilities.PREFERRED_MODAL_DIALOG_TEXT_WIDTH,
                StaticUtilities.MAX_MODAL_DIALOG_TEXT_WIDTH);
        String[] substrings = StaticUtilities.getLineBreakSubstrings(
        		formattedString);
        JPanel textPanel = new JPanel(new GridLayout(substrings.length, 1,
        		0, 0));
        for (int i = 0; i < substrings.length; i++) {
        	textPanel.add(new LabelWidget(substrings[i]));
        }
        
        String urlAndUser = url + " [" + userName + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        LabelWidget urlLabel = new LabelWidget(urlAndUser);
        Font newFont = urlLabel.getFont().deriveFont(Font.BOLD);
        urlLabel.setFont(newFont);
        
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        this.getContentPane().add(urlLabel);
        this.getContentPane().add(textPanel);
        this.getContentPane().add(buttonsPanel);
        layout.setConstraints(urlLabel, new GridBagConstraints(0, 0, 1, 1,
        		0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
        		new Insets(6, 4, 10, 4), 0, 0));
        layout.setConstraints(textPanel, new GridBagConstraints(0, 1, 1, 1,
        		0.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
        		new Insets(10, 4, 5, 4), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 2, 1, 1,
        		0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
        		new Insets(5, 4, 4, 4), 0, 0));
        this.pack();
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }
    
    private void yesPressed() {
    	response = PendingChangesDialog.YES;
    	this.dispose();
    }
    
    private void noPressed() {
    	response = PendingChangesDialog.NO;
    	this.dispose();
    }
    
    private void cancelPressed() {
    	response = PendingChangesDialog.CANCEL;
    	this.dispose();
    }
    
    public int getResponse() {
    	return response;
    }
}  
