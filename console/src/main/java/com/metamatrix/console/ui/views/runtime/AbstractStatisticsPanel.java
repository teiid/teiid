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

package com.metamatrix.console.ui.views.runtime;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JPanel;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public abstract class AbstractStatisticsPanel<T> extends JPanel {

	protected TextFieldWidget[] textFieldWidgets;

	/** Get title of the panel. */
	public abstract String getTitle();

	/** Get titles of the displayed fields. */
	public abstract String[] getLabelStrings();

	/** Populate the displayed fields from the specified VMStatistics. */
	public abstract void populate(T vmStats);

	public AbstractStatisticsPanel() {
		super();
	}

	protected void init() {
		this.setBorder(new TitledBorder(getTitle()));
		GridBagLayout layout = new GridBagLayout();
		this.setLayout(layout);

		String[] labelStrings = getLabelStrings();
		int nfields = labelStrings.length;

		textFieldWidgets = new TextFieldWidget[nfields];
		LabelWidget[] labelWidgets = new LabelWidget[nfields];

		for (int i = 0; i < nfields; i++) {
			labelWidgets[i] = new LabelWidget(labelStrings[i]);
			textFieldWidgets[i] = new TextFieldWidget(0);
			textFieldWidgets[i].setEditable(false);
			this.add(labelWidgets[i]);
		}

		for (int i = 0; i < nfields; i++) {
			this.add(textFieldWidgets[i]);
		}

		for (int i = 0; i < nfields; i++) {
			layout.setConstraints(labelWidgets[i], new GridBagConstraints(0, i,
					1, 1, 0.0, 0.0, GridBagConstraints.EAST,
					GridBagConstraints.NONE, new Insets(4, 4, 4, 4), 0, 0));
		}

		for (int i = 0; i < nfields; i++) {
			layout.setConstraints(textFieldWidgets[i],
					new GridBagConstraints(1, i, 1, 1, 1.0, 0.0,
							GridBagConstraints.CENTER,
							GridBagConstraints.HORIZONTAL, new Insets(4, 4, 4,
									4), 0, 0));
		}
	}

}
