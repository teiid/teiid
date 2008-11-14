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

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import javax.swing.JDialog;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.text.Document;

import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public class ServerURLsDialog  extends JDialog  {
    private static final String TITLE = "Server URLs"; //$NON-NLS-1$

    private static final String SAVE = "Save"; //$NON-NLS-1$
    private static final String CANCEL = "Cancel"; //$NON-NLS-1$
    private static final String ADD = "Add..."; //$NON-NLS-1$
    private static final String REMOVE = "Remove"; //$NON-NLS-1$

    private java.util.List /*<String>*/ urlNamesList;
    private JList urlList;
    private JRadioButton jrbDefault = new JRadioButton("Set as default URL", //$NON-NLS-1$
            false);
    private JRadioButton jrbUseLastLogin = new JRadioButton(
            "Use last URL as default", false); //$NON-NLS-1$
    private ButtonWidget removeButton = new ButtonWidget(REMOVE);
    private ButtonWidget addButton = new ButtonWidget(ADD);
    private ButtonWidget saveButton = new ButtonWidget(SAVE);
    private ButtonWidget cancelButton = new ButtonWidget(CANCEL);
    private String currentDefaultURL = null;
    private boolean useLastLogin = false;
    private PanelState savedPanelState;
    private boolean programmaticChange = false;

    public ServerURLsDialog(Frame owner) {
        super(owner, TITLE, true);
        this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
    }

    public ServerURLsDialog() {
        super();
    }

   // **********************
    // Component Construction
    // **********************
    public void createComponent() {
        this.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent e) {
                panelClosing();
            }
        });
        jrbDefault.addItemListener(new ItemListener() {
            public void itemStateChanged(ItemEvent ev) {
                if (jrbDefault.isSelected()) {
                    jrbUseLastLogin.setSelected(false);
                    useLastLogin = false;
                    if (urlList != null) {
                        String selectedItem = (String)urlList.getSelectedValue();
                        if (selectedItem != null) {
                            currentDefaultURL = selectedItem;
                        }
                    }
                }
                checkButtonEnabling();
            }
        });
        jrbUseLastLogin.addItemListener(new ItemListener() {
            public void itemStateChanged(ItemEvent ev) {
                if (jrbUseLastLogin.isSelected()) {
                    jrbDefault.setSelected(false);
                    useLastLogin = true;
                }
                checkButtonEnabling();
            }
        });
        urlList = new JList();
        urlList.addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(ListSelectionEvent se) {
                urlListSelectionChanged();
            }
        });
        addButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                addURL();
                checkButtonEnabling();
            }
        });
        removeButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                removeURL();
                checkButtonEnabling();
            }
        });
        saveButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                savePreferences();
                panelClosing();

            }
        });
        saveButton.setEnabled(false);
        getRootPane().setDefaultButton(saveButton);
        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                cancelButtonPressed();
            }
        });
        doTheLayout();
        loadPreferences();
        savedPanelState = currentPanelState();
        checkButtonEnabling();
        pack();
        Dimension tempSize = this.getSize();
        Dimension newSize = new Dimension(Math.max(tempSize.width,
                (int)(Toolkit.getDefaultToolkit().getScreenSize().width * 0.4)),
                tempSize.height);
        this.setSize(newSize);
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    private void doTheLayout() {
        GridBagLayout layout = new GridBagLayout();
        getContentPane().setLayout(layout);

        JPanel upperPanel = new JPanel();
        upperPanel.setBorder(new TitledBorder("")); //$NON-NLS-1$
        getContentPane().add(upperPanel);
        layout.setConstraints(upperPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 4, 4, 4), 0, 0));

        JPanel saveCancelButtonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
        saveCancelButtonsPanel.add(saveButton);
        saveCancelButtonsPanel.add(cancelButton);
        getContentPane().add(saveCancelButtonsPanel);
        layout.setConstraints(saveCancelButtonsPanel, new GridBagConstraints(
                0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.NONE, new Insets(4, 4, 4, 4), 0, 0));

        GridBagLayout ul = new GridBagLayout();
        upperPanel.setLayout(ul);

        JPanel urlsPanel = new JPanel();
        GridBagLayout urlsLayout = new GridBagLayout();
        urlsPanel.setLayout(urlsLayout);
        upperPanel.add(urlsPanel);
        ul.setConstraints(urlsPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 4, 2, 4), 0, 0));

        LabelWidget urlsLabel = new LabelWidget("Server URLs:"); //$NON-NLS-1$
        urlsPanel.add(urlsLabel);
        urlsLayout.setConstraints(urlsPanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 4), 0, 0));
        JScrollPane listJSP = new JScrollPane(urlList);
        urlsPanel.add(listJSP);
        urlsLayout.setConstraints(listJSP, new GridBagConstraints(1, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 4, 0, 0), 0, 0));

        JPanel defaultsPanel = new JPanel();
        GridBagLayout dl = new GridBagLayout();
        defaultsPanel.setLayout(dl);
        defaultsPanel.add(jrbDefault);
        defaultsPanel.add(jrbUseLastLogin);
        dl.setConstraints(jrbDefault, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, 2, 0, 2), 0, 0));
        dl.setConstraints(jrbUseLastLogin, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(0, 2, 0, 2), 0, 0));

        upperPanel.add(defaultsPanel);
        ul.setConstraints(defaultsPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(0, 0, 4, 0), 0, 0));

        JPanel addRemoveButtonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
        addRemoveButtonsPanel.add(addButton);
        addRemoveButtonsPanel.add(removeButton);
        upperPanel.add(addRemoveButtonsPanel);
        ul.setConstraints(addRemoveButtonsPanel, new GridBagConstraints(
                0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.NONE, new Insets(4, 0, 8, 0), 0, 0));
    }

    private void addURL() {
//        String newURL = JOptionPane.showInputDialog(this, "Enter a URL", //$NON-NLS-1$
//                "Add URL", JOptionPane.PLAIN_MESSAGE); //$NON-NLS-1$
		AddURLDialog dlg = new AddURLDialog(urlNamesList);
		dlg.show();
		String newURL = dlg.getURL();
        if (newURL != null) {
            urlNamesList.add(newURL);
            urlList.setListData(urlNamesList.toArray());
            int index = urlNamesList.indexOf(newURL);
            urlList.setSelectedIndex(index);
        }
    }

    /**
        removeURL removes the selected url from the list
    */
    private void removeURL(){
        Object o = urlList.getSelectedValue();
        urlNamesList.remove(o);
        urlList.setListData(urlNamesList.toArray());
    }

    /**
        urlListSelectionChanged is fired when a different url in the JList
        is selected
    */
    private void urlListSelectionChanged() {
        if (urlList.getSelectedIndex() >= 0) {
            String currentURL = (String)urlList.getSelectedValue();
            jrbDefault.setSelected(currentURL != null &&
                                   currentURL.equals(currentDefaultURL) &&
                                   ! useLastLogin);
        }
        checkButtonEnabling();
    }

    private void checkButtonEnabling() {
        if (!programmaticChange) {
            if (urlList != null) {
                programmaticChange = true;
                if (urlList.getSelectedIndex() >= 0) {
                    removeButton.setEnabled(true);
                    jrbDefault.setEnabled(true);
                } else {
                    removeButton.setEnabled(false);
                    jrbDefault.setSelected(false);
                    jrbDefault.setEnabled(false);
                }
                programmaticChange = false;
            }
        }
        saveButton.setEnabled(!panelStateMatchesStartingState());
    }

    private boolean panelStateMatchesStartingState() {
        return currentPanelState().equals(savedPanelState);
    }

    private PanelState currentPanelState() {
        String[] urls;
        if (urlNamesList != null) {
            urls = new String[urlNamesList.size()];
            Iterator it = urlNamesList.iterator();
            for (int i = 0; it.hasNext(); i++) {
                urls[i] = (String)it.next();
            }
        } else {
            urls = new String[0];
        }
        return new PanelState(urls, currentDefaultURL,
                jrbUseLastLogin.isSelected());
    }

    private void loadPreferences() {
        currentDefaultURL = StaticProperties.getDefaultURL();
        useLastLogin = StaticProperties.getUseLastURLAsDefault();

        jrbUseLastLogin.setSelected(useLastLogin);        
        if (useLastLogin) {
            selectURL(StaticProperties.getCurrentURL());
        } else {
            selectURL(currentDefaultURL);
        }
    }
    
    /**
     * Set the UI to display the specified URL as selected.
     * @param targetURL  URL to select. 
     */
    private void selectURL(String targetURL) {
        java.util.List urls = StaticProperties.getURLsCopy();
        urlNamesList = urls;
        if (urlNamesList == null) {
            urlNamesList = new ArrayList();
        }
        urlList.setListData(urlNamesList.toArray());
        urlList.setVisibleRowCount(5);
        
        
        boolean matchFound = false;
        Iterator it = urls.iterator();
        while (it.hasNext() && (!matchFound)) {
            String url = (String)it.next();
            if (url.equalsIgnoreCase(targetURL)) {
                urlList.setSelectedValue(url, true);
                matchFound = true;
            }
        }
        
        if (matchFound) {
            urlListSelectionChanged();
        }
    }
    

    private void savePreferences(){        
        StaticProperties.setURLs(urlNamesList, currentDefaultURL, useLastLogin);
        // save the properties back to the file
        try {
            StaticProperties.saveProperties();
        } catch (ExternalException e) {
            ExceptionUtility.showExternalFailureMessage("Updating Preferences", e); //$NON-NLS-1$
        }
    }

    private void cancelButtonPressed() {
        panelClosing();
    }

    private void panelClosing() {
        this.dispose();
    }

    public static void viewPreferences(Frame parent) {
        ServerURLsDialog dialog = new ServerURLsDialog(parent);
        dialog.createComponent();
        dialog.show();
    }
}//end ServerURLsDialog



class PanelState {
    private String[] urls;
    private String defaultURL;
    private boolean useLastLogin;

    public PanelState(String[] urls, String defaultURL, boolean useLastLogin) {
        this.urls = urls;
        this.defaultURL = defaultURL;
        this.useLastLogin = useLastLogin;
    }

    public String[] getURLs() {
        return urls;
    }

    public String getDefaultURL() {
        return defaultURL;
    }

    public boolean getUseLastLogin() {
        return useLastLogin;
    }

    public boolean equals(Object obj) {
        boolean equals = false;
        if (obj == this) {
            equals = true;
        } else if (obj instanceof PanelState) {
            PanelState ps = (PanelState)obj;
            if (this.useLastLogin == ps.getUseLastLogin()) {
                String argDefaultURL = ps.getDefaultURL();
                if (this.useLastLogin || (((defaultURL == null) &&
                        (argDefaultURL == null))  || ((defaultURL != null) &&
                        (argDefaultURL != null) &&
                        defaultURL.equals(argDefaultURL)))) {
                    if (Arrays.equals(urls, ps.getURLs())) {
                        equals = true;
                    }
                }
            }
        }
        return equals;
    }
}//end PanelState




class AddURLDialog extends JDialog {
	private java.util.List /*<String>*/ urlNamesList;
	private TextFieldWidget urlText;
	private ButtonWidget okButton;
	private ButtonWidget cancelButton;
	private String url = null;
	
	public AddURLDialog(java.util.List /*<String>*/ urls) {
		super(ConsoleMainFrame.getInstance(), "Add URL",  //$NON-NLS-1$
				true);
		urlNamesList = urls;
		createComponent();
	}
	
	private void createComponent() {
		GridBagLayout layout = new GridBagLayout();
		getContentPane().setLayout(layout);
		LabelWidget addLabel = new LabelWidget("Enter a URL:"); //$NON-NLS-1$
		getContentPane().add(addLabel);
		layout.setConstraints(addLabel, new GridBagConstraints(0, 0, 1, 1,
				0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
				new Insets(10, 20, 0, 20), 0, 0));
		urlText = new TextFieldWidget(40);
		urlText.getDocument().addDocumentListener(new DocumentListener() {
			public void changedUpdate(DocumentEvent ev) {
				textChanged(ev.getDocument());
			}
			public void insertUpdate(DocumentEvent ev) {
				textChanged(ev.getDocument());
			}
			public void removeUpdate(DocumentEvent ev) {
				textChanged(ev.getDocument());
			}
		});
		getContentPane().add(urlText);
		layout.setConstraints(urlText, new GridBagConstraints(0, 1, 1, 1,
				0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
				new Insets(0, 20, 5, 20), 0, 0));
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
		getContentPane().add(buttonsPanel);
		layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 2, 1, 1,
				0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
				new Insets(5, 0, 5, 0), 0, 0));
		pack();
		setLocation(StaticUtilities.centerFrame(getSize()));
        this.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent e) {
                cancelPressed();
            }
        });
	}
		
	private void cancelPressed() {
		url = null;
		dispose();
	}
	
	private void okPressed() {
		url = urlText.getText().trim();
		dispose();
	}
		
	private void textChanged(Document doc) {
		int docLen = doc.getLength();
		String str = null;
		try {
			str = doc.getText(0, docLen);
		} catch (Exception ex) {
			//cannot occur
		}
		str = str.trim();
		if (str.length() == 0) {
			okButton.setEnabled(false);
		} else {
			if (matchesExistingURL(str)) {
				okButton.setEnabled(false);
			} else {
				okButton.setEnabled(true);
			}
		}
	}
	
	private boolean matchesExistingURL(String str) {
		boolean matches = false;
		Iterator it = urlNamesList.iterator();
		while ((!matches) && it.hasNext()) {
			String curURL = (String)it.next();
			matches = str.equalsIgnoreCase(curURL);
		}
		return matches;
	}
	
	public String getURL() {
		return url;
	}
}//end AddURLDialog
