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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;

import javax.swing.JPanel;
import javax.swing.border.CompoundBorder;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.deploy.util.PropertyConstants;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class DeployedServicePanel
    extends DetailPanel
    implements PropertyConstants {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private CheckBox chkEssential;
    private PropertiedObjectPanel pnlProps;
    private JPanel pnlPropsOuter;
    private TextFieldWidget txfHost;
    private TextFieldWidget txfProc;
    private TextFieldWidget txfProd;
    private TextFieldWidget txfPsc;
    private TextFieldWidget txfService;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private PropertiedObjectEditor editor;
    
    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public DeployedServicePanel(ConnectionInfo connInfo) {
        super(connInfo);
        setTitle(getString("dvp.title")); //$NON-NLS-1$
    }

    public DeployedServicePanel(ConfigurationID theConfigId,
                                ConnectionInfo connInfo) throws ExternalException {
		this(connInfo);
        setConfigId(theConfigId);
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected JPanel construct(boolean readOnly) {
        JPanel pnl = new JPanel(new GridBagLayout());

        LabelWidget lblHost = DeployPkgUtils.createLabel("dvp.lblHost"); //$NON-NLS-1$
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblHost, gbc);

        txfHost = DeployPkgUtils.createTextField("type.hostname"); //$NON-NLS-1$
        txfHost.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfHost, gbc);

        LabelWidget lblProc = DeployPkgUtils.createLabel("dvp.lblProc"); //$NON-NLS-1$
        gbc.gridx = 2;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblProc, gbc);

        txfProc = DeployPkgUtils.createTextField("type.processname"); //$NON-NLS-1$
        txfProc.setEditable(false);
        gbc.gridx = 3;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfProc, gbc);

        LabelWidget lblProd = DeployPkgUtils.createLabel("dvp.lblProduct"); //$NON-NLS-1$
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.insets = new Insets(3, 3, 10, 3);
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblProd, gbc);

        txfProd = DeployPkgUtils.createTextField("type.productname"); //$NON-NLS-1$
        txfProd.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfProd, gbc);

        LabelWidget lblPsc = DeployPkgUtils.createLabel("dvp.lblPsc"); //$NON-NLS-1$
        gbc.gridx = 2;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblPsc, gbc);

        txfPsc = DeployPkgUtils.createTextField("type.pscname"); //$NON-NLS-1$
        txfPsc.setEditable(false);
        gbc.gridx = 3;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfPsc, gbc);

        LabelWidget lblService = DeployPkgUtils.createLabel("dvp.lblService"); //$NON-NLS-1$
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblService, gbc);

        txfService = DeployPkgUtils.createTextField("type.servicename"); //$NON-NLS-1$
        txfService.setEditable(false);
        gbc.gridx = 1;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(txfService, gbc);

        LabelWidget lblEssential = DeployPkgUtils.createLabel("dvp.lblEssential"); //$NON-NLS-1$
        gbc.gridx = 2;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.EAST;
        pnl.add(lblEssential, gbc);

        chkEssential = new CheckBox();
        chkEssential.setEnabled(false);
        gbc.gridx = 3;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.WEST;
        pnl.add(chkEssential, gbc);

        pnlPropsOuter = new JPanel(new GridLayout(1, 1));
        TitledBorder tBorder = new TitledBorder(getString("dvp.pnlProps.title")); //$NON-NLS-1$
        pnlPropsOuter.setBorder(new CompoundBorder(tBorder,
                               DeployPkgUtils.EMPTY_BORDER));


        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnl.add(pnlPropsOuter, gbc);

        // initialize the properties editor
        try {
            editor = getConfigurationManager().getPropertiedObjectEditor();
            pnlProps = new PropertiedObjectPanel(editor, getEncryptor());
            pnlProps.createComponent();
            pnlProps.setColumnHeaderNames(
                getString("pop.propertyname.hdr"), //$NON-NLS-1$
                getString("pop.propertyvalue.hdr")); //$NON-NLS-1$
            pnlProps.setReadOnlyForced(true);
            pnlPropsOuter.add(pnlProps);
        }
        catch (ExternalException theException) {
            ExceptionUtility.showMessage(
                getString("msg.configmgrproblem", //$NON-NLS-1$
                          new Object[] {getClass(), "construct"}), //$NON-NLS-1$
                theException);
            LogManager.logError(LogContexts.PSCDEPLOY,
                                theException,
                                getClass() + ":construct"); //$NON-NLS-1$
        }

        return pnl;
    }

    public void setDomainObject(
        Object theDomainObject,
        Object[] theAncestors) {

        if (!(theDomainObject instanceof DeployedComponent)) {
            throw new IllegalArgumentException(
                getString("msg.invalidclass", //$NON-NLS-1$
                          new Object[] {"DeployedComponent", //$NON-NLS-1$
                                        theDomainObject.getClass()}));
        }

        ServiceComponentDefnID id =
            ((DeployedComponent)theDomainObject).getServiceComponentDefnID();
        Configuration config = 
            getConfigurationManager().getConfig(getConfigId());
        ServiceComponentDefn service =
            (ServiceComponentDefn)config.getComponentDefn(id);
        
        super.setDomainObject(service, theAncestors);
        setTitleSuffix(service.toString());
        
        txfService.setText(service.toString());
        
        Host host = (Host)theAncestors[2];
        txfHost.setText(host.getName());
        
        VMComponentDefn process = (VMComponentDefn)theAncestors[1];
        txfProc.setText(process.getName());
        
        ProductServiceConfig psc = (ProductServiceConfig)theAncestors[0];
        txfPsc.setText(psc.getName());
        
        ProductType product = getConfigurationManager().getProduct(psc);
        txfProd.setText(product.getName());
        
        String essential = service.getProperty(ESSENTIAL_PROP);
        if (essential == null) {
            essential = ""; //$NON-NLS-1$
        }
        chkEssential.setSelected((new Boolean(essential)).booleanValue());
        
        PropertiedObject propObj = getConfigurationManager()
        .getPropertiedObjectForComponentObject(service);
        pnlProps.setNameColumnHeaderWidth(0);
        pnlProps.setPropertiedObject(propObj);
    }

}
