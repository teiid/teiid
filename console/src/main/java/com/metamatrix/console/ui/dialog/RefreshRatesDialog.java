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

import java.awt.*;
import java.awt.event.*;

import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.ui.layout.WorkspaceController;
import com.metamatrix.console.util.*;
import com.metamatrix.toolbox.ui.widget.*;

/**
 * Dialog consisting of sub-tabs for specifying various user preferences.
 *
 */
public class RefreshRatesDialog extends JDialog  {


    private static final String TITLE = "Refresh Rates"; //$NON-NLS-1$

    private static final String SUMMARY = "Summary "; //$NON-NLS-1$
    private static final String SESSION = "Sessions"; //$NON-NLS-1$
    private static final String QUERY   = "Queries"; //$NON-NLS-1$
    private static final String SYSLOG  = "Log Viewer"; //$NON-NLS-1$
    private static final String RESOURCE_POOLS = "Connection Pools"; //$NON-NLS-1$
    private static final int NUM_ITEMS = 5;
    private static final int SUMMARY_POSIT = 0;
    private static final int SESSION_POSIT = 1;
    private static final int QUERY_POSIT = 2;
    private static final int SYSLOG_POSIT = 3;
    private static final int RESOURCE_POOLS_POSIT = 4;

    private static final String SERVER_REFRESH = "Server Data Refresh"; //$NON-NLS-1$
    private static final String AUTO = "Auto"; //$NON-NLS-1$

    private static final String SECLBL = " sec."; //$NON-NLS-1$

    private static final String SAVE = "Save"; //$NON-NLS-1$
    private static final String CANCEL = "Cancel"; //$NON-NLS-1$
    private static final String RESET = "Reset"; //$NON-NLS-1$


    private static final String BLANK = " "; //$NON-NLS-1$

    private CheckBox chkbxSummary = new CheckBox("  " + SUMMARY); //$NON-NLS-1$
    private CheckBox chkbxSession = new CheckBox("  " + SESSION); //$NON-NLS-1$
    private CheckBox chkbxQuery = new CheckBox("  " + QUERY); //$NON-NLS-1$
    private CheckBox chkbxSyslog  = new CheckBox("  " + SYSLOG); //$NON-NLS-1$
    private CheckBox chkbxResourcePools = new CheckBox("  " + RESOURCE_POOLS); //$NON-NLS-1$

    private TextFieldWidget txtSummary = new TextFieldWidget(4);
    private TextFieldWidget txtSession = new TextFieldWidget(4);
    private TextFieldWidget txtQuery = new TextFieldWidget(4);
    private TextFieldWidget txtSyslog   = new TextFieldWidget(4);
    private TextFieldWidget txtResourcePools = new TextFieldWidget(4);
    
    private ButtonWidget resetButton;
    private ButtonWidget saveButton;

	private TextFieldInfo[] savedState;
	
	public RefreshRatesDialog(Frame owner) {
        super(owner, TITLE, true);
        this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
    }

    public RefreshRatesDialog() {
        super();
    }

    // **********************
    // Component Construction
    // **********************
   public void createComponent() {
        this.getContentPane().setLayout(new BorderLayout());

        JPanel border = new JPanel(new BorderLayout());
        border.setBorder(new EmptyBorder(5,5,5,5));
        border.add(buildRefreshPanel(), BorderLayout.CENTER);
        border.add(buildButtonPanel(), BorderLayout.SOUTH);

        this.getContentPane().add(border, BorderLayout.CENTER);

        Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
        pack();
        Dimension mySize = getSize();
        setLocation((screen.width - mySize.width) / 2, 
        		((screen.height - mySize.height) / 2));

        this.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent e) {
                panelClosing();
            }
        });

        chkbxSummary.setSelected(false);
        chkbxSession.setSelected(false);
        chkbxQuery.setSelected(false);
        chkbxSyslog.setSelected(false);
        chkbxResourcePools.setSelected(false);

        loadPreferences();
        
        chkbxSummary.addItemListener(new ItemListener() {
        	public void itemStateChanged(ItemEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        chkbxSession.addItemListener(new ItemListener() {
        	public void itemStateChanged(ItemEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        chkbxQuery.addItemListener(new ItemListener() {
        	public void itemStateChanged(ItemEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        chkbxSyslog.addItemListener(new ItemListener() {
        	public void itemStateChanged(ItemEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        chkbxResourcePools.addItemListener(new ItemListener() {
        	public void itemStateChanged(ItemEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        txtSummary.getDocument().addDocumentListener(new DocumentListener() {
        	public void changedUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void insertUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void removeUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        txtSession.getDocument().addDocumentListener(new DocumentListener() {
        	public void changedUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void insertUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void removeUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        txtQuery.getDocument().addDocumentListener(new DocumentListener() {
        	public void changedUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void insertUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void removeUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        txtSyslog.getDocument().addDocumentListener(new DocumentListener() {
        	public void changedUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void insertUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void removeUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
        txtResourcePools.getDocument().addDocumentListener(new DocumentListener() {
        	public void changedUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void insertUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        	public void removeUpdate(DocumentEvent ev) {
        		adjustSaveAndResetState();
        	}
        });
	}

    private JPanel buildRefreshPanel() {
        JPanel lblPanel = new JPanel();
        lblPanel.setLayout(new BorderLayout());
        lblPanel.setBorder(new EmptyBorder(0,39,0,0));
        LabelWidget lblAuto = new LabelWidget(AUTO);
        lblPanel.add(lblAuto, BorderLayout.WEST);

        JPanel grid = new JPanel(new GridLayout(NUM_ITEMS, 3));
        grid.setBorder(new EmptyBorder(0,40,10,0));

		for (int i = 0; i < NUM_ITEMS; i++) {
			switch (i) {
				case SUMMARY_POSIT:
					grid.add(chkbxSummary);
					grid.add(txtSummary);
					grid.add(getSecLabel());
					break;
				case SESSION_POSIT:
					grid.add(chkbxSession);
					grid.add(txtSession);
					grid.add(getSecLabel());
					break;
				case QUERY_POSIT:
					grid.add(chkbxQuery);
					grid.add(txtQuery);
					grid.add(getSecLabel());
					break;
				case SYSLOG_POSIT:
					grid.add(chkbxSyslog);
					grid.add(txtSyslog);
					grid.add(getSecLabel());
					break;
				case RESOURCE_POOLS_POSIT:
					grid.add(chkbxResourcePools);
					grid.add(txtResourcePools);
					grid.add(getSecLabel());
			}
		}
        GridBagLayout l = new GridBagLayout();
        GridBagConstraints gbc = new GridBagConstraints();

        JPanel rPanel = new JPanel(l);
        rPanel.setBorder(new TitledBorder(SERVER_REFRESH));

        //common settings
        gbc.insets = new Insets(5,5,5,5);
        gbc.ipadx = 5;
        gbc.ipady = 5;
        gbc.gridx = 0;
        gbc.weightx = 1.0;
        gbc.gridheight = 1;
        gbc.gridwidth = 1;
        gbc.anchor = GridBagConstraints.NORTH;

        //settings for JPanel with label
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridy = 0;
        gbc.weighty = 0.0;
        l.setConstraints(lblPanel, gbc);
        rPanel.add(lblPanel);

        //settings for JPanel with grid of JComponents for setting refresh
        //rates
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridy = 1;
        gbc.weighty = 1.0;
        l.setConstraints(grid, gbc);
        rPanel.add(grid);

        return rPanel;
    }

    private LabelWidget getSecLabel() {
        LabelWidget lbl = new LabelWidget(SECLBL);
        return lbl;
    }
    private JPanel buildButtonPanel() {
        JPanel buttonPanel = new JPanel();
        GridBagLayout bl = new GridBagLayout();
        buttonPanel.setLayout(bl);
        JPanel innerButtonPanel = new JPanel();
        innerButtonPanel.setLayout(new GridLayout(1, 3, 8, 0));
        saveButton = new ButtonWidget(SAVE);
        saveButton.setEnabled(false);
        innerButtonPanel.add(saveButton);
        ButtonWidget cancelButton = new ButtonWidget(CANCEL);
        cancelButton.setEnabled(true);
        innerButtonPanel.add(cancelButton);
        resetButton = new ButtonWidget(RESET);
        resetButton.addActionListener(new ActionListener() {
        	public void actionPerformed(ActionEvent ev) {
        		resetPressed();
        	}
        });
        resetButton.setEnabled(false);
        innerButtonPanel.add(resetButton);
        saveButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                boolean saved = savePreferences();
                if (saved) {
                    panelClosing();
                }
            }
        });
        getRootPane().setDefaultButton(saveButton);
        
        // cancel means remove anything in the addSet and close this dialog
        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                cancelButtonPressed();
            }
        });
        buttonPanel.add(innerButtonPanel);
        bl.setConstraints(innerButtonPanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(10, 10, 10, 10), 0, 0));
        return buttonPanel;
    }

  //**********************
  //  Loading Preferences
  //**********************
    private void loadPreferences() {
//        Properties prop =
            StaticProperties.getProperties();
        String value;

        // Session
        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_ENABLED_SESSION);
        if ((value != null) && (value.trim().length() > 0)) {
            if (value.trim().equals(StaticProperties.TRUE)) {
                chkbxSession.setSelected(true);
            } else {
                chkbxSession.setSelected(false);
            }
        } else {
            chkbxSession.setSelected(false);
        }
        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_RATE_SESSION);
        if (value != null) {
        	value = value.trim();
        	if (value.equals("0")) { //$NON-NLS-1$
        		value = ""; //$NON-NLS-1$
        	}
        }
        if ((value != null) && (value.length() > 0)) {
            txtSession.setText(value.trim());
        } else {
            txtSession.setText(BLANK);
        }

        // Query
        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_ENABLED_QUERY);
        if ((value != null) && (value.trim().length() > 0)) {
            if (value.trim().equals(StaticProperties.TRUE)) {
                chkbxQuery.setSelected(true);
            } else {
                chkbxQuery.setSelected(false);
            }
        } else {
            chkbxQuery.setSelected(false);
        }
        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_RATE_QUERY);
        if (value != null) {
        	value = value.trim();
        	if (value.equals("0")) { //$NON-NLS-1$
        		value = ""; //$NON-NLS-1$
        	}
        }
        if ((value != null) && (value.length() > 0)) {
            txtQuery.setText(value.trim());
        } else {
            txtQuery.setText(BLANK);
        }
        
        // Summary
        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_ENABLED_SUMMARY);
        if ((value != null) && (value.trim().length() > 0)) {
            if (value.trim().equals(StaticProperties.TRUE)) {
                chkbxSummary.setSelected(true);
            } else {
                chkbxSummary.setSelected(false);
            }
        } else {
            chkbxSummary.setSelected(false);
        }

        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_RATE_SUMMARY);
        if (value != null) {
        	value = value.trim();
        	if (value.equals("0")) { //$NON-NLS-1$
        		value = ""; //$NON-NLS-1$
        	}
        }
        if ((value != null) && (value.length() > 0)) {
            txtSummary.setText(value.trim());
        } else {
            txtSummary.setText(BLANK);
        }
        
        // Syslog
        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_ENABLED_SYSLOG);
        if ((value != null) && (value.trim().length() > 0)) {
            if (value.trim().equals(StaticProperties.TRUE)) {
                chkbxSyslog.setSelected(true);
            } else {
                chkbxSyslog.setSelected(false);
            }
        } else {
            chkbxSyslog.setSelected(false);
        }
		value = StaticProperties.getProperty(
				StaticProperties.REFRESH_RATE_SYSLOG);
        if (value != null) {
        	value = value.trim();
        	if (value.equals("0")) { //$NON-NLS-1$
        		value = ""; //$NON-NLS-1$
        	}
        }
        if ((value != null) && (value.length() > 0)) {
            txtSyslog.setText(value.trim());
        } else {
            txtSyslog.setText(BLANK);
        }

		// Resource pools
        value = StaticProperties.getProperty(
        		StaticProperties.REFRESH_ENABLED_RESOURCE_POOLS);
        if ((value != null) && (value.trim().length() > 0)) {
            if (value.trim().equals(StaticProperties.TRUE)) {
                chkbxResourcePools.setSelected(true);
            } else {
                chkbxResourcePools.setSelected(false);
            }
        } else {
            chkbxResourcePools.setSelected(false);
        }
		value = StaticProperties.getProperty(
				StaticProperties.REFRESH_RATE_RESOURCE_POOLS);
        if (value != null) {
        	value = value.trim();
        	if (value.equals("0")) { //$NON-NLS-1$
        		value = ""; //$NON-NLS-1$
        	}
        }
        if ((value != null) && (value.length() > 0)) {
            txtResourcePools.setText(value.trim());
        } else {
            txtResourcePools.setText(BLANK);
        }
        
        saveState(getState());
    }

    private boolean refreshRateOK(TextFieldInfo info) {
        boolean okay;
        String text = info.getTextField().getText().trim();
        if ((text.length() == 0) && (!info.getCheckBox().isSelected())) {
            okay = true;
        } else {
            Integer ii = null;
            try {
                ii = new Integer(text);
            } catch (Exception ex) {
            }
            if ((ii == null) || (ii.intValue() <
                    StaticProperties.MIN_REFRESH_RATE) || 
                    (ii.intValue() > StaticProperties.MAX_REFRESH_RATE)) {
                okay = false;
            } else {
                okay = true;
            }
        }
        if (!okay) {
            StaticUtilities.displayModalDialogWithOK("Illegal Value", //$NON-NLS-1$
                    "Illegal value entered for " + info.getName() + //$NON-NLS-1$
                    ".  Must be an integer from " + //$NON-NLS-1$
                    StaticProperties.MIN_REFRESH_RATE + " to " + //$NON-NLS-1$
                    StaticProperties.MAX_REFRESH_RATE + "."); //$NON-NLS-1$
            info.getTextField().requestFocus();
        }
        return okay;
    }

    /**
        savePreferences executes the saving all the preferences persistently
    */
    private boolean savePreferences() {
    	TextFieldInfo[] tfi = getState();
        int i = 0;
        boolean okay = true;
        while ((i < tfi.length) && okay) {
            okay = refreshRateOK(tfi[i]);
            i++;
        }

        if (okay) {

            // save data back to properties

            StaticProperties.setProperty(
            		StaticProperties.REFRESH_ENABLED_SESSION,
            		cvtBooleanToString(chkbxSession.isSelected()));
            StaticProperties.setProperty(StaticProperties.REFRESH_RATE_SESSION,
            		txtSession.getText().trim());

            StaticProperties.setProperty(StaticProperties.REFRESH_ENABLED_QUERY,
            		cvtBooleanToString(chkbxQuery.isSelected()));
            StaticProperties.setProperty(StaticProperties.REFRESH_RATE_QUERY,
            		txtQuery.getText().trim());

            StaticProperties.setProperty(
            		StaticProperties.REFRESH_ENABLED_SUMMARY,
            		cvtBooleanToString(chkbxSummary.isSelected()));
            StaticProperties.setProperty(StaticProperties.REFRESH_RATE_SUMMARY,
                    txtSummary.getText().trim());

            StaticProperties.setProperty(
            		StaticProperties.REFRESH_ENABLED_SYSLOG,
            		cvtBooleanToString(chkbxSyslog.isSelected()));
            StaticProperties.setProperty(StaticProperties.REFRESH_RATE_SYSLOG,
                    txtSyslog.getText().trim());

            StaticProperties.setProperty(
            		StaticProperties.REFRESH_ENABLED_RESOURCE_POOLS,
            		cvtBooleanToString(chkbxResourcePools.isSelected()));
            StaticProperties.setProperty(
            		StaticProperties.REFRESH_RATE_RESOURCE_POOLS,
            		txtResourcePools.getText().trim());

            // set the state of the application based on these settings

            // save the properties back to the file
            boolean continuing = true;
            try {

                StaticProperties.saveProperties();
            } catch (ExternalException e) {
                ExceptionUtility.showExternalFailureMessage(
                		"Updating Preferences", e); //$NON-NLS-1$
                continuing = false;
            }

			if (continuing) {
				// now propagate the new values through the WorkspaceController:
				WorkspaceController.getInstance().applyAutoRefreshParmsToAll();
				saveState(tfi);
			}
        }
        return okay;
    }

	private TextFieldInfo[] getState() {
		TextFieldInfo[] tfi = new TextFieldInfo[NUM_ITEMS];
		tfi[SUMMARY_POSIT] = new TextFieldInfo(SUMMARY, txtSummary, 
				chkbxSummary);
		tfi[SESSION_POSIT] = new TextFieldInfo(SESSION, txtSession, 
				chkbxSession);
		tfi[QUERY_POSIT] = new TextFieldInfo(QUERY, txtQuery, chkbxQuery);
		tfi[SYSLOG_POSIT] = new TextFieldInfo(SYSLOG, txtSyslog, chkbxSyslog);
		tfi[RESOURCE_POOLS_POSIT] = new TextFieldInfo(RESOURCE_POOLS, 
				txtResourcePools, chkbxResourcePools);
		return tfi;
	}
			
	private void resetPressed() {
		resetState();
	}

	private void resetState() {
		for (int i = 0; i < savedState.length; i++) {
			savedState[i].getCheckBox().setSelected(
					savedState[i].isCheckBoxSelected());
			savedState[i].getTextField().setText(
					savedState[i].getTextFieldText());
		}
	}

	private void adjustSaveAndResetState() {
		TextFieldInfo[] first = savedState;
		TextFieldInfo[] second = getState();
		boolean allSame = TextFieldInfo.allEqual(first, second);
		boolean enable = (!allSame);
		resetButton.setEnabled(enable);
		saveButton.setEnabled(enable);
	}
		
	private void saveState(TextFieldInfo[] tfi) {
		savedState = tfi;
	}
		
    private String cvtBooleanToString(boolean b) {
        String str;
        if (b) {
            str = StaticProperties.TRUE;
        } else {
            str = StaticProperties.FALSE;
        }
        return str;
    }

    private void cancelButtonPressed() {
        panelClosing();
    }

    private void panelClosing() {
        this.dispose();
    }

    public static void viewPreferences(Frame parent) {
        RefreshRatesDialog dialog = new RefreshRatesDialog(parent);
        dialog.createComponent();
        dialog.show();
    }
}//end RefreshRatesDialog




class TextFieldInfo {
	public static boolean allEqual(TextFieldInfo[] first,
			TextFieldInfo[] second) {
		boolean same = false;
		if (first.length == second.length) {
			int i = 0;
			boolean mismatchFound = false;
			while ((i < first.length) && (!mismatchFound)) {
				boolean currentItemsSame = first[i].equals(second[i]);
				if (currentItemsSame) {
					i++;
				} else {
					mismatchFound = true;
				}
			}
			same = (!mismatchFound);
		}
		return same;
	}
		
    private String name;
    private TextFieldWidget textField;
    private CheckBox checkBox;
    private String textFieldText;
    private boolean checkBoxSelected;

    public TextFieldInfo(String n, TextFieldWidget tf, CheckBox cb) {
        super();
        name = n;
        textField = tf;
        checkBox = cb;
        textFieldText = textField.getText().trim();
        checkBoxSelected = checkBox.isSelected();
    }

    public String getName() {
        return name;
    }

    public TextFieldWidget getTextField() {
        return textField;
    }

    public CheckBox getCheckBox() {
        return checkBox;
    }
    
    public String getTextFieldText() {
    	return textFieldText;
    }
    
    public boolean isCheckBoxSelected() {
    	return checkBoxSelected;
    }
    
    public boolean equals(Object obj) {
    	boolean same = false;
    	if (obj instanceof TextFieldInfo) {
    		TextFieldInfo tfi = (TextFieldInfo)obj;
    		if (tfi.getName().equals(this.name)) {
    			if  (tfi.getTextField() == this.textField) {
    				if (tfi.getCheckBox() == this.checkBox) {
    					if (tfi.getTextFieldText().equals(textFieldText)) {
    						if (tfi.isCheckBoxSelected() ==
    								checkBoxSelected) {
    							same = true;
    						}
    					}
    				}
    			}
    		}
    	}
    	return same;
    }
}//end TextFieldInfo
