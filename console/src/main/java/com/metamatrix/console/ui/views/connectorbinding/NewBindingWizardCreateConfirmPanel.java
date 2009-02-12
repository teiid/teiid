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

package com.metamatrix.console.ui.views.connectorbinding;

import java.awt.*;

import javax.swing.*;
import com.metamatrix.console.ui.util.BasicWizardSubpanelContainer;
import com.metamatrix.console.ui.util.WizardInterface;
import com.metamatrix.toolbox.ui.widget.*;

// ===

public class NewBindingWizardCreateConfirmPanel extends BasicWizardSubpanelContainer {

    private JPanel pnlOuter         = new JPanel();

	private CheckBox cbxModifyPSCEnablements
        = new CheckBox( "Review the Enabled State of the new Connector Binding in the PSCs", //$NON-NLS-1$
                        CheckBox.SELECTED );


       // blather
    private String[] CREATE_CONNBIND_BLATHER = new String[] {
    		"Click Next to create this Connector Binding. " + //$NON-NLS-1$
            "After the Binding is created it will be added to all of the " + //$NON-NLS-1$
            "Connector PSCs.", //$NON-NLS-1$
            "If you wish to review the PSCs afterward and " + //$NON-NLS-1$
            "modify which PSCs have this Binding enabled, leave the checkbox " + //$NON-NLS-1$
            "checked on this page."}; //$NON-NLS-1$


    public NewBindingWizardCreateConfirmPanel(WizardInterface wizardInterface) {
        super(wizardInterface);
        init();
    }

    private void init() {
        pnlOuter.setLayout( new GridBagLayout() );
        pnlOuter.add(cbxModifyPSCEnablements, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0
            ,GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(5, 0, 5, 0), 5, 4));

        setMainContent( pnlOuter );
        setStepText(3, false, "Create the Connector Binding",  //$NON-NLS-1$
        		CREATE_CONNBIND_BLATHER);
        cbxModifyPSCEnablements.setEnabled( true );
	}

    public CheckBox getModifyPSCEnablementsControl() {
        return cbxModifyPSCEnablements;
    }

    public boolean isSelectedModifyPSCEnablementsControl() {
        return getModifyPSCEnablementsControl().isSelected();
    }
}
