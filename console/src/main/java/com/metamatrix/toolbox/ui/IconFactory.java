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

//################################################################################################################################
package com.metamatrix.toolbox.ui;

// JDK imports
import java.awt.Component;
import java.awt.Graphics;

import javax.swing.AbstractButton;
import javax.swing.ButtonModel;
import javax.swing.Icon;

/**
@since 2.0
@version 2.0
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class IconFactory implements IconConstants {
	//############################################################################################################################
	//# Static Variables                                                                                                         #
	//############################################################################################################################

	private static Icon radioButtonIcon = null;

	//############################################################################################################################
	//# Static Methods                                                                                                           #
	//############################################################################################################################

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	@since 2.0
	*/
	public static Icon getErrorIcon() {
		return UIDefaults.getInstance().getIcon(ERROR_ICON_PROPERTY);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	@since 2.0
	*/
	public static Icon getNotificationIcon() {
		return UIDefaults.getInstance().getIcon(NOTIFICATION_ICON_PROPERTY);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	@since 2.0
	*/
	public static Icon getWarningIcon() {
		return UIDefaults.getInstance().getIcon(WARNING_ICON_PROPERTY);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	@since 2.0
	*/
	public static Icon getRadioButtonIcon() {
		if (radioButtonIcon == null) {
			radioButtonIcon = new Icon() {
				public int getIconHeight() {
					return 13;
				}
				public int getIconWidth() {
					return 13;
				}
				public void paintIcon(
					final Component component,
					final Graphics canvas,
					final int x,
					final int y) {
					final ButtonModel model =
						((AbstractButton) component).getModel();
					final UIDefaults dflts = UIDefaults.getInstance();
					// Fill interior
					if ((model.isPressed() && model.isArmed())
						|| !model.isEnabled()) {
						canvas.setColor(dflts.getColor("RadioButton.background")); //$NON-NLS-1$
					} else {
						canvas.setColor(dflts.getColor("RadioButton.highlight")); //$NON-NLS-1$
					}
					canvas.fillRect(x + 2, y + 2, 8, 8);
					// Outer left arc
					canvas.setColor(dflts.getColor("RadioButton.shadow")); //$NON-NLS-1$
					canvas.drawLine(x + 4, y + 0, x + 7, y + 0);
					canvas.drawLine(x + 2, y + 1, x + 3, y + 1);
					canvas.drawLine(x + 8, y + 1, x + 9, y + 1);
					canvas.drawLine(x + 1, y + 2, x + 1, y + 3);
					canvas.drawLine(x + 0, y + 4, x + 0, y + 7);
					canvas.drawLine(x + 1, y + 8, x + 1, y + 9);
					// Outer right arc
					canvas.setColor(dflts.getColor("RadioButton.highlight")); //$NON-NLS-1$
					canvas.drawLine(x + 2, y + 10, x + 3, y + 10);
					canvas.drawLine(x + 4, y + 11, x + 7, y + 11);
					canvas.drawLine(x + 8, y + 10, x + 9, y + 10);
					canvas.drawLine(x + 10, y + 9, x + 10, y + 8);
					canvas.drawLine(x + 11, y + 7, x + 11, y + 4);
					canvas.drawLine(x + 10, y + 3, x + 10, y + 2);
					// Inner left arc
					canvas.setColor(dflts.getColor("RadioButton.darkShadow")); //$NON-NLS-1$
					canvas.drawLine(x + 4, y + 1, x + 7, y + 1);
					canvas.drawLine(x + 2, y + 2, x + 3, y + 2);
					canvas.drawLine(x + 8, y + 2, x + 9, y + 2);
					canvas.drawLine(x + 2, y + 3, x + 2, y + 3);
					canvas.drawLine(x + 1, y + 4, x + 1, y + 7);
					canvas.drawLine(x + 2, y + 8, x + 2, y + 8);
					// Inner right arc
					canvas.setColor(dflts.getColor("RadioButton.background")); //$NON-NLS-1$
					canvas.drawLine(x + 2, y + 9, x + 3, y + 9);
					canvas.drawLine(x + 4, y + 10, x + 7, y + 10);
					canvas.drawLine(x + 8, y + 9, x + 9, y + 9);
					canvas.drawLine(x + 9, y + 8, x + 9, y + 8);
					canvas.drawLine(x + 10, y + 7, x + 10, y + 4);
					canvas.drawLine(x + 9, y + 3, x + 9, y + 3);
					// Indicate whether selected
					if (model.isSelected()) {
						canvas.setColor(dflts.getColor("RadioButton.darkShadow")); //$NON-NLS-1$
						canvas.fillRect(x + 4, y + 5, 4, 2);
						canvas.fillRect(x + 5, y + 4, 2, 4);
					}
				}
			};
		}
		return radioButtonIcon;
	}
}
