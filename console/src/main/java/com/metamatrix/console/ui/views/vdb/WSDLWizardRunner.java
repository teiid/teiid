/*
 * Copyright � 2000-2005 MetaMatrix, Inc. All rights reserved.
 */
package com.metamatrix.console.ui.views.vdb;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.uddi4j.response.BusinessInfo;
import org.uddi4j.response.BusinessList;

import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.util.WSDLServletUtil;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.IconComponent;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.console.ui.util.WizardInterfaceImpl;
import com.metamatrix.console.ui.util.property.Icons;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.SavedUDDIRegistryInfo;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.uddi.exception.MMUddiException;
import com.metamatrix.uddi.util.MMUddiHelper;
import com.metamatrix.uddi.util.UddiHelper;

/**
 * @since 5.5.3
 */
public class WSDLWizardRunner {
	private final static int CONFIG_PANEL_STEP_NUM = 1;
	private final static int BUSINESS_PANEL_STEP_NUM = 2;
	private final static int WSDL_URL_PANEL_STEP_NUM = 3;
	private final static int RESULTS_PANEL_STEP_NUM = 4;

	private final static String NO_BUSINESSES = ConsolePlugin.Util.getString("WSDLWizardRunner.noBusinesses"); //$NON-NLS-1$
	private final static String NO_BUSINESSES_MSG = ConsolePlugin.Util.getString("WSDLWizardRunner.noBusinessesMsg"); //$NON-NLS-1$
	// private final static String HTTP = "http"; //$NON-NLS-1$

	private Configuration currentConfig;
	private ConfigurationManager configMgr;
	private boolean publishing;
	private String vdbName;
	private String vdbVersion;
	private SelectUDDIConfigurationPanel configPanel;
	private SelectBusinessPanel businessPanel;
	private CreateWSDLURLElementsPanel wsdlURLPanel;
	private ResultsPanel resultsPanel;
	private JPanel currentPanel;
	private boolean cancelled = false;
	private boolean finished = false;
	private WSDLWizardPanel wizardPanel = null;
	private WSDLWizardPanelDialog dialog;
	private UddiHelper uddiAPI = null;
	private String password = StringUtil.Constants.EMPTY_STRING;
	private BusinessList /* <BusinessEntity> */businesses = null;
	private boolean haveShownWSDLURLPanel = false;
	private static String DEFAULT_WEBSERVICE_CONTEXT = "/metamatrix-soap";
	

	public WSDLWizardRunner( String vdbName,
	                         String vdbVersion,
	                         Configuration currConfig,
	                         ConfigurationManager configMgr,
	                         boolean publishing ) {
		super();
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.currentConfig = currConfig;
		this.configMgr = configMgr;
		this.publishing = publishing;		
	}

	public void go() {
		wizardPanel = new WSDLWizardPanel(this);
		configPanel = new SelectUDDIConfigurationPanel(wizardPanel, CONFIG_PANEL_STEP_NUM);
		businessPanel = new SelectBusinessPanel(wizardPanel, BUSINESS_PANEL_STEP_NUM);
		wsdlURLPanel = new CreateWSDLURLElementsPanel(wizardPanel, WSDL_URL_PANEL_STEP_NUM);
		resultsPanel = new ResultsPanel(wizardPanel, publishing, RESULTS_PANEL_STEP_NUM);
		wizardPanel.addPage(configPanel);
		wizardPanel.addPage(businessPanel);
		wizardPanel.addPage(wsdlURLPanel);
		wizardPanel.addPage(resultsPanel);
		wizardPanel.getNextButton().setEnabled(false);		
		currentPanel = configPanel;
		wizardPanel.getCancelButton().addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				cancelPressed();
			}
		});
		wizardPanel.getFinishButton().addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				finishPressed();
			}
		});
		dialog = new WSDLWizardPanelDialog(this, wizardPanel, publishing);
		dialog.setVisible(true);
		if (finished && (!cancelled)) {
			// Insert any post-processing code here
		}		
	}
	
	public void dialogWindowClosing() {
		cancelled = true;
	}

	public void cancelPressed() {
		dialog.cancelPressed();
		cancelled = true;
	}

	public void finishPressed() {
		dialog.finishPressed();
		finished = true;
	}

	public boolean showNextPage() {
		boolean goingToNextPage = true;
		if (currentPanel == configPanel) {
			if (uddiAPI == null) {
				uddiAPI = new MMUddiHelper();
			}
			SavedUDDIRegistryInfo config = configPanel.getSelectedConfig();
			UDDIPasswordDialog passwordDialog = new UDDIPasswordDialog(config.getName());
			passwordDialog.setVisible(true);
			password = passwordDialog.getPassword();
			if (password == null) {
				goingToNextPage = false;
			} else {
				String inquiryUrl = config.getInquiryUrl();
				String publishUrl = config.getPublishUrl();
				businesses = null;
				try {
					StaticUtilities.startWait(dialog.getContentPane());
					businesses = uddiAPI.getAllBusinesses(inquiryUrl, publishUrl, 100);
				} catch (MMUddiException ex) {
					StaticUtilities.endWait(dialog.getContentPane());
					displayInitialUDDIAccessError(ex);
					goingToNextPage = false;
				}
				if (goingToNextPage) {
					StaticUtilities.endWait(dialog.getContentPane());
					if (businesses.getBusinessInfos().size() == 0) {
						StaticUtilities.displayModalDialogWithOK(NO_BUSINESSES, NO_BUSINESSES_MSG, JOptionPane.WARNING_MESSAGE);
						goingToNextPage = false;
					}
				}
				if (goingToNextPage) {
					businessPanel.setBusinesses(businesses);
					currentPanel = businessPanel;
					businessPanel.setForwardButtonEnabling();
				}
			}
		} else if (currentPanel == businessPanel) {
			// Insert here any checks which might prevent proceeding to next page
			if (goingToNextPage) {
				if (!haveShownWSDLURLPanel) {
					ConfigurationID configID = (ConfigurationID)currentConfig.getID();
					wsdlURLPanel.setdefaultWebServerHost("localhost");
					wsdlURLPanel.setDefaultWebServerPort("8080");
					wizardPanel.getNextButton().setEnabled(true);
					haveShownWSDLURLPanel = true;
				}
				currentPanel = wsdlURLPanel;
				wsdlURLPanel.setForwardButtonEnabling();
			}
		} else if (currentPanel == wsdlURLPanel) {
			String webServerScheme = wsdlURLPanel.getWebServerScheme();
			String webServerHost = wsdlURLPanel.getWebServerHost();
			String webServerPort = wsdlURLPanel.getWebServerPort();
			String wsdlURL = makeURL(webServerScheme, webServerHost, webServerPort);
			SavedUDDIRegistryInfo config = configPanel.getSelectedConfig();
			String userName = config.getUserName();
			String inquiryUrl = config.getInquiryUrl();
			String publishUrl = config.getPublishUrl();
			String businessKey = businessPanel.getSelectedBusinessKey();
			if (publishing) {
				MMUddiException publishingException = null;
				boolean isPublished = false;
				try {
					isPublished = uddiAPI.isPublished(userName, password, businessKey, wsdlURL, inquiryUrl, publishUrl);
				} catch (MMUddiException ex) {
					publishingException = ex;
				}
				if (publishingException == null) {
					if (isPublished) {
						resultsPanel.addIncorrectPublishingStateText();
					} else {
						try {
							uddiAPI.publish(userName, password, businessKey, wsdlURL, inquiryUrl, publishUrl);
						} catch (MMUddiException ex) {
							publishingException = ex;
						}
					}
				}
				if ((!isPublished) && (publishingException == null)) {
					resultsPanel.addSuccessText();
					wizardPanel.getBackButton().setVisible(false);
				} else {
					if (publishingException != null) {
						resultsPanel.addErrorHandlingSubcomponents(publishingException);
					}
				}
			} else {
				MMUddiException unpublishingException = null;
				boolean isPublished = false;
				try {
					isPublished = uddiAPI.isPublished(userName, password, businessKey, wsdlURL, inquiryUrl, publishUrl);
				} catch (MMUddiException ex) {
					unpublishingException = ex;
				}
				if (unpublishingException == null) {
					if (!isPublished) {
						resultsPanel.addIncorrectPublishingStateText();
					} else {
						try {
							uddiAPI.unPublish(userName, password, businessKey, wsdlURL, inquiryUrl, publishUrl);
						} catch (MMUddiException ex) {
							unpublishingException = ex;
						}
					}
				}
				if (isPublished && (unpublishingException == null)) {
					resultsPanel.addSuccessText();
					wizardPanel.getBackButton().setVisible(false);
				} else {
					if (unpublishingException != null) {
						resultsPanel.addErrorHandlingSubcomponents(unpublishingException);
					}
				}
			}
			// Insert any checks which might prevent proceeding to next page here
			if (goingToNextPage) {
				currentPanel = resultsPanel;
			}
		}
		boolean cancelVisible = (currentPanel != resultsPanel);
		wizardPanel.getCancelButton().setVisible(cancelVisible);
		return goingToNextPage;
	}

	public void showPreviousPage() {
		// Everything on the new current page will be as we left it which was a state that allowed
		// enabling of the Next button. So it is safe to go ahead and enable the 'Next' button.
		if (currentPanel == businessPanel) {
			currentPanel = configPanel;
			configPanel.enableForwardButton(true);
		} else if (currentPanel == wsdlURLPanel) {
			currentPanel = businessPanel;
			businessPanel.enableForwardButton(true);
		} else if (currentPanel == resultsPanel) {
			currentPanel = wsdlURLPanel;
			wsdlURLPanel.enableForwardButton(true);
		}
		boolean cancelVisible = (currentPanel != resultsPanel);
		wizardPanel.getCancelButton().setVisible(cancelVisible);
	}

	private String makeURL( String scheme,
	                        String host,
	                        String port ) {
		List list = new ArrayList();
		list.add(configMgr.getConnection().getURL());
		return WSDLServletUtil.formatURL(scheme, host, port, DEFAULT_WEBSERVICE_CONTEXT, list, vdbName, vdbVersion); //$NON-NLS-1$
	}

	private void displayInitialUDDIAccessError( MMUddiException ex ) {
		// TODO-- what to display may depend on contents of exception. If can discern that
		// unable to connect to UDDI, then so state and do not show option to show error dialog.
		// Else possibly put up modal dialog with "View Error Dialog" button, but then dispose of
		// pop-up before going to error dialog.
		UDDIInitialAccessErrorDialog dlg = new UDDIInitialAccessErrorDialog(ConsoleMainFrame.getInstance(), ex);
		dlg.setVisible(true);
	}
}// end WSDLWizardRunner

class WSDLWizardPanel extends WizardInterfaceImpl {
	private WSDLWizardRunner controller;

	public WSDLWizardPanel( WSDLWizardRunner cntrlr ) {
		super();
		this.controller = cntrlr;
	}

	public void showNextPage() {
		boolean continuing = controller.showNextPage();
		if (continuing) {
			super.showNextPage();
		}
	}

	public void showPreviousPage() {
		controller.showPreviousPage();
		super.showPreviousPage();
	}
}// end WSDLWizardPanel

class SelectUDDIConfigurationPanel extends BasicWizardSubpanelContainer implements UDDIConfigurationsHandler {
	private final static String SELECT_CONFIG = ConsolePlugin.Util.getString("WSDLWizardRunner.selectAConfig"); //$NON-NLS-1$
	private final static String CONFIGURATIONS = ConsolePlugin.Util.getString("WSDLWizardRunner.configurations"); //$NON-NLS-1$
	private final static String UDDI_CONFIG = ConsolePlugin.Util.getString("UDDIConfigurationsDialog.configName") + ':'; //$NON-NLS-1$
	private final static String INQUIRY_URL = ConsolePlugin.Util.getString("UDDIConfigurationsDialog.uddiInquiryUrl") + ':'; //$NON-NLS-1$
	private final static String PUBLISH_URL = ConsolePlugin.Util.getString("UDDIConfigurationsDialog.uddiPublishUrl") + ':'; //$NON-NLS-1$
	private final static String USER = ConsolePlugin.Util.getString("UDDIConfigurationsDialog.userName") + ':'; //$NON-NLS-1$
	private final static int TOP_INSET = 10;
	private final static int LEFT_INSET = 10;
	private final static int CONFIG_ROW_BOTTOM_INSET = 5;
	private final static int SECOND_COL_LEFT_INSET = 4;
	private final static int FIELDS_PANEL_BETWEEN_ROWS_INSET = 4;

	private JPanel thePanel;
	private JComboBox configsCombo;
	private JButton configsButton;
	private JTextField inquiryUrlField;
	private JTextField publishUrlField;
	private JTextField userField;
	private java.util.List /* <SavedUDDIRegistryInfo> */items;

	public SelectUDDIConfigurationPanel( WizardInterface wizardInterface,
	                                     int stepNum ) {
		super(wizardInterface);
		super.setStepText(stepNum, SELECT_CONFIG);
		SavedUDDIRegistryInfo[] savedItems = StaticProperties.getUDDIRegistryInfo();
		items = new ArrayList(savedItems.length);
		for (int i = 0; i < savedItems.length; i++) {
			items.add(savedItems[i]);
		}
		thePanel = createPanel();
		super.setMainContent(thePanel);		
	}
		 
	private JPanel createPanel() {
		JPanel panel = new JPanel();
		GridBagLayout layout = new GridBagLayout();
		panel.setLayout(layout);
		JLabel configsLabel = new LabelWidget(UDDI_CONFIG);
		panel.add(configsLabel);
		layout.setConstraints(configsLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHWEST,
		                                                           GridBagConstraints.NONE,
		                                                           new Insets(TOP_INSET, LEFT_INSET, CONFIG_ROW_BOTTOM_INSET, 0),
		                                                           0, 1));
		String[] configNamesArray = createConfigNamesArray();
		configsCombo = new JComboBox(configNamesArray);
		configsCombo.addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				configSelectionChanged();
			}
		});
		configsCombo.setEditable(false);
		panel.add(configsCombo);
		layout.setConstraints(configsCombo, new GridBagConstraints(1, 0, 1, 1, 1.0, 1.0, GridBagConstraints.NORTH,
		                                                           GridBagConstraints.HORIZONTAL,
		                                                           new Insets(TOP_INSET, SECOND_COL_LEFT_INSET,
		                                                                      CONFIG_ROW_BOTTOM_INSET, 0), 0, 0));
		configsButton = new ButtonWidget(CONFIGURATIONS);
		configsButton.addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				configsButtonPressed();
			}
		});
		panel.add(configsButton);
		layout.setConstraints(configsButton, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTHWEST,
		                                                            GridBagConstraints.NONE, new Insets(TOP_INSET, 4,
		                                                                                                CONFIG_ROW_BOTTOM_INSET,
		                                                                                                10), 0, 0));
		JPanel fieldsPanel = new JPanel();
		panel.add(fieldsPanel);
		layout.setConstraints(fieldsPanel, new GridBagConstraints(1, 1, 1, 1, 0.0, 4.0, GridBagConstraints.NORTH,
		                                                          GridBagConstraints.HORIZONTAL, new Insets(20, LEFT_INSET, 20,
		                                                                                                    LEFT_INSET), 0, 0));
		GridBagLayout fieldsLayout = new GridBagLayout();
		fieldsPanel.setLayout(fieldsLayout);
		JLabel hostLabel = new LabelWidget(INQUIRY_URL);
		fieldsPanel.add(hostLabel);
		fieldsLayout.setConstraints(hostLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                              GridBagConstraints.NONE,
		                                                              new Insets(0, 0, FIELDS_PANEL_BETWEEN_ROWS_INSET, 0), 0, 0));
		inquiryUrlField = new TextFieldWidget(100);
		inquiryUrlField.setEditable(false);
		fieldsPanel.add(inquiryUrlField);
		fieldsLayout.setConstraints(inquiryUrlField, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
		                                                                    GridBagConstraints.HORIZONTAL,
		                                                                    new Insets(0, SECOND_COL_LEFT_INSET,
		                                                                               FIELDS_PANEL_BETWEEN_ROWS_INSET, 0), 0, 0));
		JLabel portLabel = new LabelWidget(PUBLISH_URL);
		fieldsPanel.add(portLabel);
		fieldsLayout.setConstraints(portLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                              GridBagConstraints.NONE,
		                                                              new Insets(FIELDS_PANEL_BETWEEN_ROWS_INSET, 0,
		                                                                         FIELDS_PANEL_BETWEEN_ROWS_INSET, 0), 0, 0));
		publishUrlField = new TextFieldWidget(100);
		publishUrlField.setEditable(false);
		fieldsPanel.add(publishUrlField);
		fieldsLayout.setConstraints(publishUrlField, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
		                                                                    GridBagConstraints.HORIZONTAL,
		                                                                    new Insets(FIELDS_PANEL_BETWEEN_ROWS_INSET,
		                                                                               SECOND_COL_LEFT_INSET,
		                                                                               FIELDS_PANEL_BETWEEN_ROWS_INSET, 0), 0, 0));
		JLabel userLabel = new LabelWidget(USER);
		fieldsPanel.add(userLabel);
		fieldsLayout.setConstraints(userLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                              GridBagConstraints.NONE,
		                                                              new Insets(FIELDS_PANEL_BETWEEN_ROWS_INSET, 0, 0, 0), 0, 0));
		userField = new TextFieldWidget();
		userField.setEditable(false);
		fieldsPanel.add(userField);
		fieldsLayout.setConstraints(userField, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
		                                                              GridBagConstraints.HORIZONTAL,
		                                                              new Insets(FIELDS_PANEL_BETWEEN_ROWS_INSET,
		                                                                         SECOND_COL_LEFT_INSET, 0, 0), 0, 0));
		return panel;
	}

	private String[] createConfigNamesArray() {
		String[] array = new String[items.size() + 1];
		array[0] = StringUtil.Constants.EMPTY_STRING;
		Iterator it = items.iterator();
		for (int i = 1; it.hasNext(); i++) {
			SavedUDDIRegistryInfo curItem = (SavedUDDIRegistryInfo)it.next();
			array[i] = curItem.getName();
		}
		return array;
	}

	private void configSelectionChanged() {
		setForwardButtonEnabling();
	}

	private void setForwardButtonEnabling() {
		int index = configsCombo.getSelectedIndex();
		if (index >= 1) {
			SavedUDDIRegistryInfo selectedItem = (SavedUDDIRegistryInfo)items.get(index - 1);
			inquiryUrlField.setText(selectedItem.getInquiryUrl());
			publishUrlField.setText(selectedItem.getPublishUrl());
			userField.setText(selectedItem.getUserName());
			enableForwardButton(true);			
		} else {
			inquiryUrlField.setText(StringUtil.Constants.EMPTY_STRING);
			publishUrlField.setText(StringUtil.Constants.EMPTY_STRING);
			userField.setText(StringUtil.Constants.EMPTY_STRING);
			enableForwardButton(false);
		}
	}

	private void configsButtonPressed() {
		runConfigsDialog();
	}

	private void runConfigsDialog() {
		SavedUDDIRegistryInfo[] array = new SavedUDDIRegistryInfo[items.size()];
		Iterator it = items.iterator();
		for (int i = 0; it.hasNext(); i++) {
			array[i] = (SavedUDDIRegistryInfo)it.next();
		}
		UDDIConfigurationsDialog dialog = new UDDIConfigurationsDialog(ConsoleMainFrame.getInstance(), this, array);
		dialog.setVisible(true);
	}

	public void addedConfiguration( SavedUDDIRegistryInfo config ) {
		DefaultComboBoxModel model = (DefaultComboBoxModel)configsCombo.getModel();
		items.add(config);
		updateConfigs();
		model.addElement(config.getName());
		selectItem(config);
	}

	public void removedConfiguration( SavedUDDIRegistryInfo config ) {
		DefaultComboBoxModel model = (DefaultComboBoxModel)configsCombo.getModel();
		items.remove(config);
		updateConfigs();
		model.removeElement(config.getName());
	}

	public void editedConfiguration( SavedUDDIRegistryInfo config ) {
		// Cannot change the name when editing the item, so we do not have to change the combo box model
		int matchIndex = matchIndexOfConfigName(config);
		if (matchIndex >= 0) {
			items.set(matchIndex, config);
			updateConfigs();
			selectItem(config);
		}
	}

	public void unchangedConfiguration( SavedUDDIRegistryInfo config ) {
		selectItem(config);
	}

	private void selectItem( SavedUDDIRegistryInfo config ) {
		int matchIndex = matchIndexOfConfigName(config);
		if (matchIndex >= 0) {
			// Offset by 1 to account for blank entry at start of combo
			configsCombo.setSelectedIndex(matchIndex + 1);
		}
	}

	private int matchIndexOfConfigName( SavedUDDIRegistryInfo config ) {
		String configName = config.getName();
		int matchIndex = -1;
		int index = 0;
		int numItems = items.size();
		while ((matchIndex < 0) && (index < numItems)) {
			SavedUDDIRegistryInfo curConfig = (SavedUDDIRegistryInfo)items.get(index);
			String curConfigName = curConfig.getName();
			if (configName.equals(curConfigName)) {
				matchIndex = index;
			} else {
				index++;
			}
		}
		return matchIndex;
	}

	private void updateConfigs() {
		SavedUDDIRegistryInfo[] array = new SavedUDDIRegistryInfo[items.size()];
		Iterator it = items.iterator();
		for (int i = 0; it.hasNext(); i++) {
			array[i] = (SavedUDDIRegistryInfo)it.next();
		}
		StaticProperties.setUDDIRegistryInfo(array);
	}

	public SavedUDDIRegistryInfo getSelectedConfig() {
		SavedUDDIRegistryInfo selection = null;
		int selectedIndex = configsCombo.getSelectedIndex();
		// Offset by 1 to account for blank entry at top of combo box
		if (selectedIndex >= 1) {
			selection = (SavedUDDIRegistryInfo)items.get(selectedIndex - 1);
		}
		return selection;
	}

	/**
	 * Overridden to enable the forward button only if the file selection is valid.
	 * 
	 * @see com.metamatrix.console.ui.util.BasicWizardSubpanelContainer#resolveForwardButton()
	 * @since 5.5.3
	 */
	public void resolveForwardButton() {
		setForwardButtonEnabling();
	}

	public Dimension getPreferredSize() {
		Dimension size = super.getPreferredSize();
		// System.err.println("in SelectUDDIConfigurationPanel getPreferredSize(), returning " + size);
		return size;
	}
}// end SelectUDDIConfigurationPanel

class SelectBusinessPanel extends BasicWizardSubpanelContainer {
	private final static String SELECT_A_BUSINESS = ConsolePlugin.Util.getString("WSDLWizardRunner.selectABusiness"); //$NON-NLS-1$
	private final static String SELECT_A_BUSINESS_LABEL = SELECT_A_BUSINESS + ':';
	private final static String BUSINESS_NAME = ConsolePlugin.Util.getString("WSDLWizardRunner.businessName"); //$NON-NLS-1$
	private final static String BUSINESS_KEY = ConsolePlugin.Util.getString("WSDLWizardRunner.businessKey"); //$NON-NLS-1$
	private final static String DESCRIPTION_LABEL = ConsolePlugin.Util.getString("WSDLWizardRunner.description"); //$NON-NLS-1$ 

	private final static int BUSINESS_NAME_COL_INDEX = 0;
	private final static int BUSINESS_KEY_COL_INDEX = 1;
	// Order must correspond to column index constants
	private final static String[] COL_HEADERS = new String[] {BUSINESS_NAME, BUSINESS_KEY};

	private final static int LEFT_INSET = 10;
	private final static int RIGHT_INSET = 10;

	private JPanel thePanel;
	private BusinessList /* <BusinessEntity> */businesses;
	private JTable table;
	private JTextArea descriptionArea;

	public SelectBusinessPanel( WizardInterface wizardInterface,
	                            int stepNum ) {
		super(wizardInterface);
		super.setStepText(stepNum, SELECT_A_BUSINESS);
		thePanel = createPanel();
		super.setMainContent(thePanel);
	}

	private JPanel createPanel() {
		table = new SelectBusinessPanelTable(Arrays.asList(COL_HEADERS));
		table.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			public void valueChanged( ListSelectionEvent ev ) {
				if (!ev.getValueIsAdjusting()) {
					tableRowSelectionChanged();
				}
			}
		});
		((TableWidget)table).setEditable(false);

		JPanel panel = new JPanel();
		GridBagLayout layout = new GridBagLayout();
		panel.setLayout(layout);
		JLabel selectBusinessLabel = new LabelWidget(SELECT_A_BUSINESS_LABEL);
		panel.add(selectBusinessLabel);
		layout.setConstraints(selectBusinessLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                                  GridBagConstraints.NONE, new Insets(10, LEFT_INSET, 10,
		                                                                                                      RIGHT_INSET), 0, 0));
		JScrollPane tableScrollPane = new JScrollPane(table);
		panel.add(tableScrollPane);
		layout.setConstraints(tableScrollPane, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
		                                                              GridBagConstraints.BOTH, new Insets(0, LEFT_INSET, 10,
		                                                                                                  RIGHT_INSET), 0, 0));
		JLabel descriptionLabel = new LabelWidget(DESCRIPTION_LABEL);
		panel.add(descriptionLabel);
		layout.setConstraints(descriptionLabel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                               GridBagConstraints.NONE, new Insets(0, LEFT_INSET, 0,
		                                                                                                   RIGHT_INSET), 0, 0));
		descriptionArea = new JTextArea();
		descriptionArea.setEditable(false);
		descriptionArea.setLineWrap(true);
		descriptionArea.setWrapStyleWord(true);
		panel.add(descriptionArea);
		layout.setConstraints(descriptionArea, new GridBagConstraints(0, 3, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
		                                                              GridBagConstraints.BOTH, new Insets(0, LEFT_INSET, 0,
		                                                                                                  RIGHT_INSET), 0, 0));
		return panel;
	}

	private void tableRowSelectionChanged() {
		int selectedRow = table.getSelectedRow();
		if (selectedRow < 0) {
			descriptionArea.setText(StringUtil.Constants.EMPTY_STRING);
		} else {
			BusinessInfo selectedBusiness = businesses.getBusinessInfos().get(selectedRow);
			String desc = selectedBusiness.getDefaultDescriptionString();
			descriptionArea.setText(desc);
		}
		setForwardButtonEnabling();
	}

	public void setBusinesses( BusinessList busList ) {
		this.businesses = busList;
		DefaultTableModel model = (DefaultTableModel)table.getModel();
		model.setRowCount(0);
		Iterator it = this.businesses.getBusinessInfos().getBusinessInfoVector().iterator();
		while (it.hasNext()) {
			BusinessInfo business = (BusinessInfo)it.next();
			String businessKey = business.getBusinessKey();
			String businessName = business.getDefaultNameString();
			String[] rowData = new String[2];
			rowData[BUSINESS_NAME_COL_INDEX] = businessName;
			rowData[BUSINESS_KEY_COL_INDEX] = businessKey;
			model.addRow(rowData);			
		}
	}

	public String getSelectedBusinessKey() {
		String key = null;
		int selectedIndex = table.getSelectedRow();
		if (selectedIndex >= 0) {
			key = (String)table.getModel().getValueAt(selectedIndex, BUSINESS_KEY_COL_INDEX);
		}
		return key;
	}

	public void setForwardButtonEnabling() {
		int selectedRow = table.getSelectedRow();
		enableForwardButton((selectedRow >= 0));
	}

	/**
	 * Overridden to enable the forward button only if the file selection is valid.
	 * 
	 * @see com.metamatrix.console.ui.util.BasicWizardSubpanelContainer#resolveForwardButton()
	 * @since 5.5.3
	 */
	public void resolveForwardButton() {
		setForwardButtonEnabling();
	}

	public Dimension getPreferredSize() {
		Dimension size = super.getPreferredSize();
		// System.err.println("in SelectBusinessPanel getPreferredSize(), returning " + size);
		return size;
	}
}// end SelectBusinessPanel

class SelectBusinessPanelTable extends TableWidget {
	public SelectBusinessPanelTable( java.util.List columns ) {
		super(columns);
	}

	public Dimension getPreferredSize() {
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		Dimension preferredSize = super.getPreferredSize();
		int preferredWidth = Math.max(preferredSize.width, (screenSize.width / 2));
		return new Dimension(preferredWidth, preferredSize.height);
	}
}// end SelectBusinessPanelTable

/**
 * Panel to set and display SOAP URL connection properties
 * @since 5.5.3
 */
class CreateWSDLURLElementsPanel extends BasicWizardSubpanelContainer {
	private final static String TITLE = ConsolePlugin.Util.getString("WSDLWizardRunner.setSOAPProperties"); //$NON-NLS-1$ 
	private final static String DESC = ConsolePlugin.Util.getString("WSDLWizardRunner.setSOAPPropertiesDesc"); //$NON-NLS-1$ 
	private final static String HOST = ConsolePlugin.Util.getString("WSDLWizardRunner.webServerHost"); //$NON-NLS-1$
	private final static String PORT = ConsolePlugin.Util.getString("WSDLWizardRunner.webServerPort"); //$NON-NLS-1$
	private final static String DEFAULTS = ConsolePlugin.Util.getString("WSDLWizardRunner.resetToDefaults"); //$NON-NLS-1$

	private final static int BETWEEN_LABELS_VERT_GAP = 2;

	private JPanel thePanel;
	private String defaultWebServerPort;
	private String defaultWebServerHost;
	private JTextField inquiryUrlField;
	private JTextField publishUrlField;

	/**
	 * Constructor
	 * @param wizardInterface
	 * @param stepNum
	 */
	public CreateWSDLURLElementsPanel( WizardInterface wizardInterface,
	                                   int stepNum ) {
		super(wizardInterface);
		super.setStepText(stepNum, TITLE);
		thePanel = createPanel();
		super.setMainContent(thePanel);
	}

	private JPanel createPanel() {
		JPanel panel = new JPanel();
		GridBagLayout layout = new GridBagLayout();
		panel.setLayout(layout);
		JLabel desc = new LabelWidget(DESC);
		panel.add(desc);
		layout.setConstraints(desc, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                   GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
		JPanel textPanel = new JPanel();
		panel.add(textPanel);
		layout.setConstraints(textPanel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
		                                                        GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
		GridBagLayout textPanelLayout = new GridBagLayout();
		textPanel.setLayout(textPanelLayout);
		JLabel hostLabel = new LabelWidget(HOST);
		textPanel.add(hostLabel);
		textPanelLayout.setConstraints(hostLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                                 GridBagConstraints.NONE, new Insets(0, 0, 0, 4), 0, 0));
		DocumentListener docListener = new DocumentListener() {
			public void changedUpdate( DocumentEvent ev ) {
				fieldsChanged();
			}

			public void removeUpdate( DocumentEvent ev ) {
				fieldsChanged();
			}

			public void insertUpdate( DocumentEvent ev ) {
				fieldsChanged();
			}
		};
		inquiryUrlField = new TextFieldWidget(50);
		inquiryUrlField.getDocument().addDocumentListener(docListener);
		textPanel.add(inquiryUrlField);
		textPanelLayout.setConstraints(inquiryUrlField,
		                               new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                      GridBagConstraints.NONE, new Insets(0, 0, BETWEEN_LABELS_VERT_GAP,
		                                                                                          0), 0, 0));
		JLabel portLabel = new LabelWidget(PORT);
		textPanel.add(portLabel);
		textPanelLayout.setConstraints(portLabel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                                 GridBagConstraints.NONE, new Insets(0, 0, 0, 4), 0, 0));
		publishUrlField = new TextFieldWidget(20);
		publishUrlField.getDocument().addDocumentListener(docListener);
		textPanel.add(publishUrlField);
		textPanelLayout.setConstraints(publishUrlField,
		                               new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                      GridBagConstraints.NONE, new Insets(BETWEEN_LABELS_VERT_GAP, 0, 0,
		                                                                                          0), 0, 0));
		JButton defaultsButton = new ButtonWidget(DEFAULTS);
		defaultsButton.addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				defaultsPressed();
			}
		});
		panel.add(defaultsButton);
		layout.setConstraints(defaultsButton, new GridBagConstraints(0, 2, 1, 1, 0.0, 1.0, GridBagConstraints.NORTH,
		                                                             GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
		return panel;
	}

	private void fieldsChanged() {
		setForwardButtonEnabling();
	}

	public void setForwardButtonEnabling() {
		String inquiryUrlText = inquiryUrlField.getText().trim();
		String publishUrlText = publishUrlField.getText().trim();
		boolean enabling = ((inquiryUrlText.length() > 0) && (publishUrlText.length() > 0));
		enableForwardButton(enabling);
	}

	/**
	 * Overridden to enable the forward button only if the file selection is valid.
	 * 
	 * @see com.metamatrix.console.ui.util.BasicWizardSubpanelContainer#resolveForwardButton()
	 * @since 5.5.3
	 */
	public void resolveForwardButton() {
		setForwardButtonEnabling();
	}

	private void defaultsPressed() {
		inquiryUrlField.setText(defaultWebServerHost);
		publishUrlField.setText(defaultWebServerPort);
	}

	public void setDefaultWebServerPort( String port ) {
		defaultWebServerPort = port;
		publishUrlField.setText(defaultWebServerPort);
	}

	public void setdefaultWebServerHost( String host ) {
		defaultWebServerHost = host;
		inquiryUrlField.setText(defaultWebServerHost);
	}

	public String getWebServerScheme() {
		return inquiryUrlField.getText().trim();
	}

	public String getWebServerHost() {
		return inquiryUrlField.getText().trim();
	}

	public String getWebServerPort() {
		return publishUrlField.getText().trim();
	}

	public Dimension getPreferredSize() {
		Dimension size = super.getPreferredSize();
		// System.err.println("in CreateWSDLURLElementsPanel getPreferredSize(), returning " + size);
		return size;
	}
}// end CreateWSDLURLElementsPanel

class ResultsPanel extends BasicWizardSubpanelContainer {
	private final static String SUCCESSFULLY_PUBLISHED = ConsolePlugin.Util.getString("WSDLWizardRunner.publishingSuccessfulMsg"); //$NON-NLS-1$
	private final static String SUCCESSFULLY_UNPUBLISHED = ConsolePlugin.Util.getString("WSDLWizardRunner.unpublishingSuccessfulMsg"); //$NON-NLS-1$
	private final static String ALREADY_PUBLISHED = ConsolePlugin.Util.getString("WSDLWizardRunner.wsdlAlreadyPublishedMsg"); //$NON-NLS-1$
	private final static String NOT_PUBLISHED = ConsolePlugin.Util.getString("WSDLWizardRunner.noWSDLPublishedMsg"); //$NON-NLS-1$
	private final static String ERROR_1 = ConsolePlugin.Util.getString("WSDLWizardRunner.errorMsg1"); //$NON-NLS-1$
	private final static String ERROR_2 = ConsolePlugin.Util.getString("WSDLWizardRunner.errorMsg2"); //$NON-NLS-1$
	private final static String PUBLISH = ConsolePlugin.Util.getString("WSDLWizardRunner.publish"); //$NON-NLS-1$
	private final static String UNPUBLISH = ConsolePlugin.Util.getString("WSDLWizardRunner.unpublish"); //$NON-NLS-1$ 
	private final static String VIEW_ERROR_DIALOG = ConsolePlugin.Util.getString("WSDLWizardRunner.viewErrorDialog"); //$NON-NLS-1$ 
	private final static String ERROR_DLG_HEADER = ConsolePlugin.Util.getString("WSDLWizardRunner.errorDlgHeader"); //$NON-NLS-1$                                                                                

	private JPanel thePanel;
	private boolean publishing;
	private Throwable throwable;

	public ResultsPanel( WizardInterface wizardInterface,
	                     boolean publishing,
	                     int stepNum ) {
		super(wizardInterface);
		super.setStepText(stepNum, "Results"); //$NON-NLS-1$ 
		thePanel = new JPanel();
		this.publishing = publishing;
		super.setMainContent(thePanel);
	}

	public void addSuccessText() {
		thePanel.removeAll();
		GridBagLayout layout = new GridBagLayout();
		thePanel.setLayout(layout);
		String text;
		if (publishing) {
			text = SUCCESSFULLY_PUBLISHED;
		} else {
			text = SUCCESSFULLY_UNPUBLISHED;
		}
		JLabel label = new LabelWidget(text);
		thePanel.add(label);
		layout.setConstraints(label, new GridBagConstraints(0, 0, 1, 1, 0.0, 1.0, GridBagConstraints.NORTH,
		                                                    GridBagConstraints.NONE, new Insets(20, 10, 10, 10), 0, 0));
	}

	public void addIncorrectPublishingStateText() {
		thePanel.removeAll();
		GridBagLayout layout = new GridBagLayout();
		thePanel.setLayout(layout);
		String text;
		IconComponent icon = new IconComponent(Icons.WARNING_ICON);
		thePanel.add(icon);
		layout.setConstraints(icon, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.NORTH,
		                                                   GridBagConstraints.NONE, new Insets(10, 20, 10, 10), 0, 0));
		if (publishing) {
			text = ALREADY_PUBLISHED;
		} else {
			text = NOT_PUBLISHED;
		}
		JLabel label = new LabelWidget(text);
		thePanel.add(label);
		layout.setConstraints(label, new GridBagConstraints(1, 0, 1, 1, 0.0, 1.0, GridBagConstraints.NORTH,
		                                                    GridBagConstraints.NONE, new Insets(10, 0, 10, 0), 0, 0));
	}

	public void addErrorHandlingSubcomponents( Throwable t ) {
		this.throwable = t;
		thePanel.removeAll();
		GridBagLayout layout = new GridBagLayout();
		thePanel.setLayout(layout);
		String errorText;
		if (publishing) {
			errorText = ERROR_1 + ' ' + PUBLISH + ' ' + ERROR_2;
		} else {
			errorText = ERROR_1 + ' ' + UNPUBLISH + ' ' + ERROR_2;
		}
		JLabel errorLabel = new LabelWidget(errorText);
		thePanel.add(errorLabel);
		layout.setConstraints(errorLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 1.0, GridBagConstraints.NORTHWEST,
		                                                         GridBagConstraints.NONE, new Insets(20, 4, 10, 4), 0, 0));
		JButton errorDialogButton = new ButtonWidget(VIEW_ERROR_DIALOG);
		thePanel.add(errorDialogButton);
		layout.setConstraints(errorDialogButton,
		                      new GridBagConstraints(0, 1, 1, 1, 0.0, 1.0, GridBagConstraints.NORTH, GridBagConstraints.NONE,
		                                             new Insets(20, 10, 10, 10), 0, 0));
		errorDialogButton.addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				viewErrorDialogPressed();
			}
		});
	}

	private void viewErrorDialogPressed() {
		ExceptionUtility.showMessage(ERROR_DLG_HEADER, throwable);
	}

	public Dimension getPreferredSize() {
		Dimension size = super.getPreferredSize();
		// System.err.println("in ResultsPanel getPreferredSize(), returning " + size);
		return size;
	}
}// end ResultsPanel

class WSDLWizardPanelDialog extends JDialog {
	private WSDLWizardRunner caller;
	private WSDLWizardPanel wizardPanel;

	public WSDLWizardPanelDialog( WSDLWizardRunner cllr,
	                              WSDLWizardPanel wizPnl,
	                              boolean publishing ) {
		super(ConsoleMainFrame.getInstance(), publishing ? "Web Services Publishing Wizard" : //$NON-NLS-1$ 
		"Web Services Unpublishing Wizard"); //$NON-NLS-1$ 
		this.caller = cllr;
		this.wizardPanel = wizPnl;
		this.setModal(true);
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing( WindowEvent ev ) {
				caller.dialogWindowClosing();
			}
		});
		init();
		//this.pack();
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		// System.err.println("screenSize is " + screenSize);
		Dimension size = this.getSize();
		// System.err.println("in WSDLWizardPanelDialog constructor, size is " + size);
		Dimension newSize = new Dimension(Math.min(Math.max(size.width, (int)(screenSize.width * 0.5)),
		                                           (int)(screenSize.width * 0.75)), Math.max(size.height,
		                                                                                     (int)(screenSize.height * 0.75)));
		this.setSize(newSize);
		this.setLocation(StaticUtilities.centerFrame(this.getSize()));
	}

	private void init() {
		GridBagLayout layout = new GridBagLayout();
		this.getContentPane().setLayout(layout);
		this.getContentPane().add(wizardPanel);
		layout.setConstraints(wizardPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
		                                                          GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
	}

	public void cancelPressed() {
		this.dispose();
	}

	public void finishPressed() {
		this.dispose();
	}
}// end WSDLWizardPanelDialog

class UDDIPasswordDialog extends JDialog {
	private final static String TITLE = ConsolePlugin.Util.getString("WSDLWizardRunner.passwordDialogTitle"); //$NON-NLS-1$
	private final static String TEXT_LINE = ConsolePlugin.Util.getString("WSDLWizardRunner.passwordText"); //$NON-NLS-1$
	private final static String PASSWORD_LABEL = ConsolePlugin.Util.getString("WSDLWizardRunner.passwordLabel"); //$NON-NLS-1$
	private final static String OK = ConsolePlugin.Util.getString("General.OK"); //$NON-NLS-1$
	private final static String CANCEL = ConsolePlugin.Util.getString("General.Cancel"); //$NON-NLS-1$
	private final static int HORIZONTAL_INSETS = 4;

	private JButton okButton;
	private JButton cancelButton;
	private JPasswordField passwordField;
	private String password = null;

	public UDDIPasswordDialog( String configName ) {
		super(ConsoleMainFrame.getInstance(), TITLE, true);
		createComponent(configName);
	}

	private void createComponent( String configName ) {
		addWindowListener(new WindowAdapter() {
			public void windowClosing( WindowEvent ev ) {
				cancelPressed();
			}
		});
		GridBagLayout layout = new GridBagLayout();
		getContentPane().setLayout(layout);
		String headerText = TEXT_LINE + ' ' + configName;
		JLabel headerLabel = new LabelWidget(headerText);
		getContentPane().add(headerLabel);
		layout.setConstraints(headerLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                          GridBagConstraints.NONE, new Insets(10, HORIZONTAL_INSETS, 10,
		                                                                                              HORIZONTAL_INSETS), 0, 0));
		JPanel passwordPanel = new JPanel();
		GridBagLayout passwordLayout = new GridBagLayout();
		passwordPanel.setLayout(passwordLayout);
		getContentPane().add(passwordPanel);
		layout.setConstraints(passwordPanel, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
		                                                            GridBagConstraints.NONE, new Insets(0, HORIZONTAL_INSETS, 0,
		                                                                                                HORIZONTAL_INSETS), 0, 0));
		JLabel passwordLabel = new LabelWidget(PASSWORD_LABEL);
		passwordPanel.add(passwordLabel);
		passwordLayout.setConstraints(passwordLabel,
		                              new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.EAST,
		                                                     GridBagConstraints.NONE, new Insets(0, 0, 0, 4), 0, 0));
		passwordField = new JPasswordField(20);
		passwordField.setPreferredSize(new Dimension(224, 20));
		passwordField.setMinimumSize(new Dimension(224, 20));

		passwordPanel.add(passwordField);
		passwordLayout.setConstraints(passwordField,
		                              new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
		                                                     GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
		passwordField.getDocument().addDocumentListener(new DocumentListener() {
			public void changedUpdate( DocumentEvent ev ) {
				passwordChanged();
			}

			public void removeUpdate( DocumentEvent ev ) {
				passwordChanged();
			}

			public void insertUpdate( DocumentEvent ev ) {
				passwordChanged();
			}
		});
		JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 10, 0));
		getContentPane().add(buttonsPanel);
		layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
		                                                           GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
		okButton = new ButtonWidget(OK);
		buttonsPanel.add(okButton);
		okButton.addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				okPressed();
			}
		});
		okButton.setEnabled(false);
		cancelButton = new ButtonWidget(CANCEL);
		buttonsPanel.add(cancelButton);
		cancelButton.addActionListener(new ActionListener() {
			public void actionPerformed( ActionEvent ev ) {
				cancelPressed();
			}
		});
		this.pack();
		this.setLocation(StaticUtilities.centerFrame(this.getSize()));
	}

	private void passwordChanged() {
		char[] passwordArray = passwordField.getPassword();
		if (passwordArray.length == 0) {
			password = null;
		} else {
			password = new String(passwordArray);
			password = password.trim();
			if (password.length() == 0) {
				password = null;
			}
		}
		okButton.setEnabled((password != null));
	}

	private void cancelPressed() {
		password = null;
		this.dispose();
	}

	private void okPressed() {
		this.dispose();
	}

	public String getPassword() {
		return password;
	}
}// end UDDIPasswordDialog
