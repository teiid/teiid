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

//#############################################################################
package com.metamatrix.console.ui.views.syslog;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.event.*;

import com.metamatrix.console.ui.layout.*;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.*;

public class MaxRecordsPerQueryPanel extends JPanel {
	public final static int MAX_TEXT_FIELD_SIZE = 5;
	public final static String MAX_ROWS_LABEL = 
			"Maximum rows allowed per query:";
	private MaxRecordsPerQueryListener listener;
	private int warningThresholdValue; //if < 0, suppress warning
	private ButtonWidget editButton;
	private TextFieldWidget maxTextField;
		
	public MaxRecordsPerQueryPanel(MaxRecordsPerQueryListener listener,
			int warningThresholdValue, int initialValue) {
		super();
		this.listener = listener;
		this.warningThresholdValue = warningThresholdValue;
		initialize(initialValue);
	}
	
	private void initialize(int initialVal) {
		GridBagLayout layout = new GridBagLayout();
		this.setLayout(layout);
		maxTextField = new TextFieldWidget(MAX_TEXT_FIELD_SIZE);
		setValue(initialVal, false);
		maxTextField.setEditable(false);
		editButton = new ButtonWidget("Edit...");
		editButton.setToolTipText("Edit the maximum number of rows.");
		editButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				editPressed();
			}
		});
		LabelWidget maxRowsLabel = new LabelWidget(MAX_ROWS_LABEL);
		JPanel innerPanel = new JPanel();
		GridBagLayout innerLayout = new GridBagLayout();
		innerPanel.setLayout(innerLayout);
		innerPanel.add(maxRowsLabel);
		innerPanel.add(maxTextField);
		innerPanel.add(editButton);
		innerLayout.setConstraints(maxRowsLabel, new GridBagConstraints(0, 0,
				1, 1, 0.0, 0.0, GridBagConstraints.EAST, 
				GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
		innerLayout.setConstraints(maxTextField, new GridBagConstraints(1, 0,
				1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
				GridBagConstraints.NONE, new Insets(0, 2, 0, 4), 0, 0));
		innerLayout.setConstraints(editButton, new GridBagConstraints(2, 0,
				1, 1, 0.0, 0.0, GridBagConstraints.WEST, 
				GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
		
		this.add(innerPanel);
		layout.setConstraints(innerPanel, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.NONE,
				new Insets(4, 4, 4, 4), 0, 0));
	}

	public void setValue(int value, boolean informListener) {
		int currentVal = -1;
		boolean numberSet = false;
		if (maxTextField.getText().length() > 0) {
			currentVal = (new Integer(maxTextField.getText())).intValue();
			numberSet = true;
		}
		if ((!numberSet) || (value != currentVal)) {
			maxTextField.setText(Integer.toString(value));
			if (informListener) {
				listener.maximumChanged(value);
			}
		}
	}
	
	public void setValue(int value) {
		setValue(value, true);
	}
	
	public int getMaxRows() {
		int max = (new Integer(maxTextField.getText())).intValue();
		return max;
	}
	
	private void editPressed() {
		int currentVal = (new Integer(maxTextField.getText())).intValue();
		EditValueDialog dialog = new EditValueDialog(currentVal);
		dialog.show();
		Integer newValInteger = dialog.getNewValue();
		if (newValInteger != null) {
			boolean continuing = true;
			int newVal = newValInteger.intValue();
			if (newVal != currentVal) {
				if (warningThresholdValue > 0) {
					if (newVal >= warningThresholdValue) {
						ValueWarningDialog warningDlg = new ValueWarningDialog(
								newVal);
						warningDlg.show();
						continuing = warningDlg.wasOKPressed();
						if (warningDlg.suppressWarningChecked()) {
							listener.doNotDisplayWarningMessage();
							warningThresholdValue = -1;
						}
					}
				}
				if (continuing) {
					maxTextField.setText(newValInteger.toString());
					listener.maximumChanged(newVal);
				}
			}
		}
	}
}//end MaxRecordsPerQueryPanel




class EditValueDialog extends JDialog {
	private ButtonWidget okButton;
	private ButtonWidget cancelButton;
	private boolean cancelled = false;
	private TextFieldWidget valueField;
	
	public EditValueDialog(int initialVal) {
		super(ConsoleMainFrame.getInstance(), "Edit Max Rows Value");
		this.setModal(true);
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent ev) {
				cancelled = true;
			}
		});
		initialize(initialVal);
	}
	
	private void initialize(int initialVal) {
		okButton = new ButtonWidget("OK");
		okButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				dispose();
			}
		});
		cancelButton = new ButtonWidget("Cancel");
		cancelButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				cancelled = true;
				dispose();
			}
		});
		valueField = new TextFieldWidget(
				MaxRecordsPerQueryPanel.MAX_TEXT_FIELD_SIZE);
		valueField.setText(Integer.toString(initialVal));
		valueField.getDocument().addDocumentListener(new DocumentListener() {
			public void changedUpdate(DocumentEvent ev) {
				textChanged();
			}
			public void insertUpdate(DocumentEvent ev) {
				textChanged();
			}
			public void removeUpdate(DocumentEvent ev) {
				textChanged();
			}
		});
		
		JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
		buttonsPanel.add(okButton);
		buttonsPanel.add(cancelButton);
		
		LabelWidget label = new LabelWidget(
				MaxRecordsPerQueryPanel.MAX_ROWS_LABEL);
		
		GridBagLayout layout = new GridBagLayout();
		this.getContentPane().setLayout(layout);
		this.getContentPane().add(label);
		this.getContentPane().add(valueField);
		this.getContentPane().add(buttonsPanel);
		layout.setConstraints(label, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
				new Insets(10, 4, 0, 0), 0, 0));
		layout.setConstraints(valueField, new GridBagConstraints(1, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
				new Insets(10, 0, 0, 4), 0, 0));
		layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 2, 1,
				0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
				new Insets(10, 4, 4, 4), 0, 0));
		this.pack();
		int height = this.getSize().height;
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		int screenHeight = screenSize.height;
		int newHeight = Math.max(height, (int)(screenHeight * 0.2));
		this.setSize(new Dimension(this.getSize().width, newHeight));
		this.setLocation(StaticUtilities.centerFrame(this.getSize()));
	}
	
	public Integer getNewValue() {
		Integer newVal = null;
		if (!cancelled) {
			newVal = new Integer(valueField.getText().trim());
		}
		return newVal;
	}
	
	private void textChanged() {
		Integer newVal = null;
		try {
			newVal = new Integer(valueField.getText().trim());
			int val = newVal.intValue();
			if (val <= 0) {
				newVal = null;
			}
		} catch (Exception ex) {
		}
		boolean valid = (newVal != null);
		okButton.setEnabled(valid);
	}
}//end EditValueDialog




class ValueWarningDialog extends JDialog {
	private final static String WARNING_TEXT = 
			"In the future, do not show this warning";
	private boolean okPressed = false;
	private ButtonWidget okButton;
	private ButtonWidget cancelButton;
	private CheckBox warningCheckBox;
	
	public ValueWarningDialog(int value) {
		super(ConsoleMainFrame.getInstance());
		String title = "Change Maximum Rows to " + value;
		this.setTitle(title);
		this.setModal(true);
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent ev) {
				//Nothing to do here, for now
			}
		});
		initialize(value);
	}
	
	private void initialize(int value) {
		GridBagLayout layout = new GridBagLayout();
		this.getContentPane().setLayout(layout);
	
		JPanel textPanel = new JPanel();
		GridBagLayout textPanelLayout = new GridBagLayout();
		textPanel.setLayout(textPanelLayout);
		LabelWidget line1 = new LabelWidget(
				"Are you sure?  Accepting a large number of result rows");
		LabelWidget line2 = new LabelWidget(
				"may degrade Console performance.");
		textPanel.add(line1);
		textPanel.add(line2);
		textPanelLayout.setConstraints(line1, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
				new Insets(0, 0, 0, 0), 0, 0));
		textPanelLayout.setConstraints(line2, new GridBagConstraints(0, 1, 1, 1,
				0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
				new Insets(0, 0, 0, 0), 0, 0));
		this.getContentPane().add(textPanel);
		
		okButton = new ButtonWidget("OK");
		okButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				okPressed();
			}
		});
		cancelButton = new ButtonWidget("Cancel");
		cancelButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				cancelPressed();
			}
		});	
		JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 20, 0));
		buttonsPanel.add(okButton);
		buttonsPanel.add(cancelButton);
		this.getContentPane().add(buttonsPanel);
		
		warningCheckBox = new CheckBox(WARNING_TEXT);
		this.getContentPane().add(warningCheckBox);
		
		layout.setConstraints(textPanel, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
				new Insets(10, 10, 10, 10), 0, 0));
		layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
				0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
				new Insets(10, 10, 10, 10), 0, 0));
		layout.setConstraints(warningCheckBox, new GridBagConstraints(0, 2, 1, 1,
				0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
				new Insets(10, 4, 4, 10), 0, 0));
		this.pack();
		int height = this.getSize().height;
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		int screenHeight = screenSize.height;
		int newHeight = Math.max(height, (int)(screenHeight * 0.2));
		this.setSize(new Dimension(this.getSize().width, newHeight));
		this.setLocation(StaticUtilities.centerFrame(this.getSize()));
	}
	
	private void okPressed() {
		okPressed = true;
		this.dispose();
	}
	
	private void cancelPressed() {
		this.dispose();
	}
	
	public boolean wasOKPressed() {
		return okPressed;
	}
	
	public boolean suppressWarningChecked() {
		return warningCheckBox.isSelected();
	}
}//end ValueWarningDialog
