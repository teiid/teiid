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

package com.metamatrix.console.ui.views.logsetup;

import javax.swing.event.ListDataEvent;
import javax.swing.event.ListDataListener;

import com.metamatrix.toolbox.ui.widget.AccumulatorPanel;

public class ContextsAccumulatorPanel extends AccumulatorPanel {
    private ContextsAccumulatorListener listener;

    public ContextsAccumulatorPanel(
            java.util.List /*<String>*/ initialAvailableContexts,
            java.util.List /*<String>*/ initialSelectedContexts,
            ContextsAccumulatorListener lsnr) {
        super(initialAvailableContexts, initialSelectedContexts);
        this.listener = lsnr;
        this.setAllowsReorderingValues(false);
        this.getAcceptButton().setVisible(false);
        this.getCancelButton().setVisible(false);
        this.getResetButton().setVisible(false);
        this.remove(this.getNavigationBar());
        this.getAvailableValuesHeader().setText("Available Contexts"); //$NON-NLS-1$
        this.getValuesHeader().setText("Selected Contexts"); //$NON-NLS-1$
        this.addListDataListener(new ListDataListener() {
            public void intervalAdded(ListDataEvent ev) {
                selectionsChanged();
            }
            public void intervalRemoved(ListDataEvent ev) {
                selectionsChanged();
            }
            public void contentsChanged(ListDataEvent ev) {
                selectionsChanged();
            }
        });
    }

    public void selectionsChanged() {
        listener.selectedContextsChanged();
    }
}

