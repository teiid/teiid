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

package com.metamatrix.console.connections;

import java.awt.*;
import java.awt.event.*;

import javax.swing.*;

import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.*;

public class RemoveConnectionDialog extends JDialog {
	private AbstractButton okButton;
	private AbstractButton cancelButton;
	private ConnectionInfo[] connections;
	private boolean cancelled = false;
	private JComboBox urlsBox = null;
	private ConnectionInfo currentConnection;
	
	public RemoveConnectionDialog(ConnectionInfo[] conns,
			ConnectionInfo currentConn) {
		super(ViewManager.getMainFrame(), "Remove Server Connection"); //$NON-NLS-1$
		this.setModal(true);
		this.connections = conns;
		this.currentConnection = currentConn;
		createComponent();
	}
	
	private void createComponent() {
		okButton = new ButtonWidget("OK"); //$NON-NLS-1$
		okButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				okPressed();
			}
		});
		okButton.setEnabled(false);
		cancelButton = new ButtonWidget("Cancel"); //$NON-NLS-1$
		cancelButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				cancelPressed();
			}
		});
		JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
		buttonsPanel.add(okButton);
		buttonsPanel.add(cancelButton);
		JPanel selectionPanel = new JPanel();
		
		int indexToSelect = -1;
		Object[] items = new Object[connections.length + 1];
		items[0] = "(select a connection)"; //$NON-NLS-1$
		for (int i = 0; i < connections.length; i++) {
			items[i + 1] = connections[i];
			if (currentConnection.equals(items[i + 1])) {
				indexToSelect = i + 1;
			}
		}
		urlsBox = new JComboBox(items);
		urlsBox.setEditable(false);
		urlsBox.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent ev) {
				comboBoxSelectionChanged();
			}
		});
		if (indexToSelect >= 0) {
			urlsBox.setSelectedIndex(indexToSelect);
			okButton.setEnabled(true);
		}
		GridBagLayout sl = new GridBagLayout();
		selectionPanel.setLayout(sl);
		LabelWidget selectionLabel = new LabelWidget(
				"Remove connection:"); //$NON-NLS-1$
		selectionPanel.add(selectionLabel);
		sl.setConstraints(selectionLabel, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
				new Insets(4, 4, 4, 2), 0, 0));
		selectionPanel.add(urlsBox);
		sl.setConstraints(urlsBox, new GridBagConstraints(1, 0, 1, 1,
				1.0, 1.0, GridBagConstraints.WEST, 
				GridBagConstraints.HORIZONTAL, new Insets(4, 2, 4, 4),
				0, 0));
				
		GridBagLayout layout = new GridBagLayout();
		this.getContentPane().setLayout(layout);
		this.getContentPane().add(selectionPanel);
		this.getContentPane().add(buttonsPanel);
		layout.setConstraints(selectionPanel, new GridBagConstraints(0, 0,
				1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
				GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
		layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1,
				1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
				GridBagConstraints.NONE, new Insets(4, 4, 4, 4), 0, 0));
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent ev) {
				cancelPressed();
			}
		});
		this.pack();
		int height = this.getSize().height;
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		int screenHeight = screenSize.height;
		int newHeight = Math.max(height, (int)(screenHeight * 0.2));
		this.setSize(new Dimension(this.getSize().width, newHeight));
		this.setLocation(StaticUtilities.centerFrame(this.getSize()));
	}
	
	public ConnectionInfo getSelectedURL() {
		ConnectionInfo ci = null;
		if (!cancelled) {
			if (connections.length == 1) {
				ci = connections[0];
			} else {
				ci = (ConnectionInfo)urlsBox.getSelectedItem();
			}
		}
		return ci;
	}
	
	private void okPressed() {
		this.dispose();
	}
	
	private void cancelPressed() {
		cancelled = true;
		this.dispose();
	}  
			
	private void comboBoxSelectionChanged() {
		int index = urlsBox.getSelectedIndex();
		okButton.setEnabled((index > 0));
	}
}
