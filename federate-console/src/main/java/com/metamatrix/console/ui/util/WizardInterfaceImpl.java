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

package com.metamatrix.console.ui.util;

import java.awt.Component;
import java.util.Iterator;

import javax.swing.AbstractButton;
import javax.swing.JPanel;

import com.metamatrix.toolbox.ui.widget.WizardPanel;

public class WizardInterfaceImpl extends WizardPanel implements WizardInterface {
    public WizardInterfaceImpl() {
        super();
    }

    public Component getOwner() {
        return this;
    }

    public AbstractButton getForwardButton() {
        AbstractButton forwardButton;
        int curPageIndex = this.getCurrentPageIndex();
        int lastPageIndex = this.getPageCount() - 1;
        if (curPageIndex == lastPageIndex) {
            forwardButton = this.getFinishButton();
        } else {
            forwardButton = this.getNextButton();
        }
        return forwardButton;
    }
    
    public Component[] getPages() {
        Component[] result;
        if (pages == null) {
            result = new Component[0];
        } else {
            result = new Component[pages.size()];
            Iterator it = pages.iterator();
            for (int i = 0; it.hasNext(); i++) {
                result[i] = (Component)it.next();
            }
        }
        return result;
    }
    
    
    /** 
     * Overridden to call resolveForwardButton() whenever a page is shown.
     * @see com.metamatrix.toolbox.ui.widget.WizardPanel#showPage(java.awt.Component)
     * @since 4.3
     */
    public void showPage(Component page) {
        super.showPage(page);
        
        JPanel pnlCurrPage = (JPanel)getCurrentPage();
        if (pnlCurrPage != null && pnlCurrPage instanceof BasicWizardSubpanelContainer) {
            ((BasicWizardSubpanelContainer)pnlCurrPage).resolveForwardButton();
        }
    }

    /** 
     * Overridden to call resolveForwardButton() whenever a page is shown.
     * @see com.metamatrix.toolbox.ui.widget.WizardPanel#showPage(int)
     * @since 4.3
     */
    public void showPage(int index) {
        super.showPage(index);

        JPanel pnlCurrPage = (JPanel)getCurrentPage();
        if (pnlCurrPage != null && pnlCurrPage instanceof BasicWizardSubpanelContainer) {
            ((BasicWizardSubpanelContainer)pnlCurrPage).resolveForwardButton();
        }
    }
}
