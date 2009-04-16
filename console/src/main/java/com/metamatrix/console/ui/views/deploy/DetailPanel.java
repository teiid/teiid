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
package com.metamatrix.console.ui.views.deploy;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.Icon;
import javax.swing.JPanel;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.util.crypto.Encryptor;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.toolbox.ui.widget.LabelWidget;

/**
 * @version 1.0
 * @author Dan Florian
 */
public abstract class DetailPanel
    extends JPanel {

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private LabelWidget lblConfig;
    private LabelWidget lblTitle;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    protected ArrayList actions = new ArrayList();
    private Object[] ancestors;
    private ConfigurationID configId;
    protected Object domainObj;
    private String title = getString("dp.title"); //$NON-NLS-1$
    private String titleSuffix = getString("dp.titlesuffix"); //$NON-NLS-1$
    private boolean includingHdr;
    //private ConfigurationManager configManager;
    private ConnectionInfo connectionInfo;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public DetailPanel(boolean includeHdr,ConnectionInfo connectionInfo) {
        super(new GridBagLayout());
        includingHdr = includeHdr;
        //this.configManager = cMgr;
        this.connectionInfo = connectionInfo;
        setBorder(DeployPkgUtils.EMPTY_BORDER);

        lblConfig = new LabelWidget();
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
//        gbc.insets = new Insets(3, 3, 3, 3);
gbc.insets = new Insets(0, 0, 0, 0);
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        if (includeHdr) {
            add(lblConfig, gbc);
        }

        lblTitle = new LabelWidget();
        Font font = lblTitle.getFont();
        lblTitle.setFont(font.deriveFont(font.getSize2D()*1.5F));
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.CENTER;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
//        gbc.insets = new Insets(3, 3, 10, 3);
gbc.insets = new Insets(0, 0, 0, 0);
        if (includeHdr) {
            add(lblTitle, gbc);
        }

		boolean canModify = UserCapabilities.getInstance()
				.canUpdateConfiguration(getConnectionInfo());
        JPanel pnl = construct((!canModify));
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
//        gbc.insets = new Insets(3, 3, 3, 3);
gbc.insets = new Insets(0, 0, 0, 0);
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        add(pnl, gbc);

        setMinimumSize(new Dimension(0, 0));
    }

    public DetailPanel(ConnectionInfo connectionInfo) {
        this(true, connectionInfo);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected boolean includingHdr() {
        return includingHdr;
    }
    
    protected abstract JPanel construct(boolean readOnly);

    public List getActions() {
        return actions;
    }

    public Object[] getAncestors() {
        return ancestors;
    }

    public ConfigurationID getConfigId() {
        return configId;
    }

    public Object getDomainObject() {
        return domainObj;
    }

    public Icon getIcon(String theKey) {
        return DeployPkgUtils.getIcon(theKey);
    }

    public int getInt(String theKey, int theDefault) {
        return DeployPkgUtils.getInt(theKey, theDefault);
    }

    public int getMnemonicChar(String theKey) {
        return DeployPkgUtils.getMnemonic(theKey);
    }

    public String getString(String theKey) {
        return DeployPkgUtils.getString(theKey);
    }

    public String getString(String theKey, Object[] theArgs) {
        return DeployPkgUtils.getString(theKey, theArgs);
    }

    public List getTreeActions() {
        return null;
    }

    public void setConfigId(ConfigurationID theConfigId) {

        configId = theConfigId;
        String iconId = null;
//        if (getConfigurationManager().isNextStartUpConfig(configId)) {
            iconId = "icon.nextstartup.big"; //$NON-NLS-1$
//        }
        if (iconId != null) {
            lblConfig.setIcon(getIcon(iconId));
            Configuration config = getConfigurationManager().getConfig(configId);
            lblConfig.setText(config.getName());
            setTitleSuffix(config.getName());
        }
    }

    public void setDomainObject(
        Object theDomainObject,
        Object[] theAncestors) {

        domainObj = theDomainObject;
        ancestors = theAncestors;
    }

    protected void setTitle(String theTitle) {
        title = theTitle;
        lblTitle.setText(title);
    }

    protected void setTitleSuffix(String theSuffix) {
        titleSuffix = theSuffix;
        lblTitle.setText(
            getString("dp.lblTitle", new Object[] {title, titleSuffix})); //$NON-NLS-1$
    }

    protected void setup(
        String theMenuEntryType,
        AbstractButton theButton,
        AbstractPanelAction theAction) {

        theAction.addComponent(theButton);
        actions.add(new MenuEntry(theMenuEntryType, theAction));
    }
    
    protected ConfigurationManager getConfigurationManager() {
    	return ModelManager.getConfigurationManager(connectionInfo);
    }
    
    protected Encryptor getEncryptor() {
        return ModelManager.getConfigurationManager(connectionInfo).getEncryptor();
    }
    
    
    protected ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }
}
