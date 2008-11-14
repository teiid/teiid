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

package com.metamatrix.toolbox.ui.widget.property;

import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;

import javax.swing.BorderFactory;
import javax.swing.JLabel;

import com.metamatrix.common.object.PropertyDefinition;

import com.metamatrix.toolbox.ui.widget.LabelWidget;

/**
 * Defines interface that custom JComponents can implement to be used in the
 * PropertyTable.  Custom components that implement or provide adapters to this
 * interface can be used generically in the table.
 */
public class PropertyDefinitionLabel extends LabelWidget {

    private final static Color normalColor = new LabelWidget().getForeground();
    private final static Font normalFont = new LabelWidget().getFont();
    private final static Font requiredFont = new Font(normalFont.getName(), Font.BOLD, normalFont.getSize());
    private final static Color requiredColor = Color.black;
    private final static Color invalidColor = Color.red;
    private final static Color requiresRestartColor = Color.BLUE;

    private final static String REQUIRES_RESTART_LABEL = " [REQUIRES RESTART]"; //$NON-NLS-1$
    private final static String REQUIRES_RESTART_TOOLTIP = " [Requires a restart or bounce of the server to take effect]"; //$NON-NLS-1$

    private static final int VERTICAL_MARGIN = PropertyComponentFactory.PROTOTYPE.getInsets().top;

    private PropertyDefinition def;
    private boolean showRequiredProperties = false;
    private static int lineY = 0;

    public PropertyDefinitionLabel(PropertyDefinition def,
                                   boolean showTooltip,
                                   boolean showRequiredProperties,
                                   boolean isInvalid) {
        super();
        String displayName = def.getDisplayName();
        if (def.getRequiresRestart()) {
            displayName = displayName + REQUIRES_RESTART_LABEL;
        }
        super.setText(displayName);
        
            
        this.def = def;
        this.showRequiredProperties = showRequiredProperties;

        if (showTooltip) {
            String text = def.getShortDescription();
            if (def.getRequiresRestart()) {
                text = text + REQUIRES_RESTART_TOOLTIP;
            }

            if (text != null && text.length() > 0) {
                setToolTipText(text);
            }
        }
        
        refreshDisplay(isInvalid);
        
        
        setVerticalAlignment(JLabel.TOP);
        setBorder(BorderFactory.createEmptyBorder(VERTICAL_MARGIN, 0, VERTICAL_MARGIN, 0));
        if (lineY == 0) {
            lineY = getPreferredSize().height / 2;
        }
        setAlignmentY(0.0f);
    }

    public void refreshDisplay(boolean isInvalid) {
        if (isInvalid) {
            setForeground(invalidColor);
        } else if (def.getRequiresRestart()) {
            setForeground(requiresRestartColor);
        } else if (showRequiredProperties && def.isRequired()) {
            setForeground(requiredColor);            
        } else {
            setForeground(normalColor);
        }
        
        if (showRequiredProperties && def.isRequired()) {
            setFont(requiredFont);
        } else {
            setFont(normalFont);
        }
        
        
    }
    
    

    public void setShowRequiredProperties(boolean showRequired) {
        this.showRequiredProperties = showRequired;
    }

    public PropertyDefinition getPropertyDefinition() {
        return this.def;
    }



    public void paint(Graphics g) {
        super.paint(g);
        // get the text in this cell and draw a grey line from the end of the text
        FontMetrics fontMetrix = g.getFontMetrics(getFont());
        int width = getWidth();
        int textWidth = fontMetrix.stringWidth(getText());
        g.setColor(Color.gray);
        g.drawLine(textWidth, lineY, width, lineY);
    }

}


