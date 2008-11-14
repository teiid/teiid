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

import java.awt.*;
import javax.swing.*;

import com.metamatrix.toolbox.ui.widget.*;

public class WizardStepTextPanel extends JPanel {
	private final static int MINIMUM_HEIGHT = 20;
	
	private int stepNum;
	private boolean optional;
	private String header;
	private String[] paragraphs;
    private JTextArea textPane;
	
	public WizardStepTextPanel(int stepNum, boolean optional, String header,
			String[] paragraphs) {
		super();
		this.stepNum = stepNum;
		this.optional = optional;
		this.header = header;
		this.paragraphs = paragraphs;
		if (this.paragraphs == null) {
			this.paragraphs = new String[0];
		}
		init();
	}
	
	private void init() {
		Font defaultFont = (new LabelWidget()).getFont();
		Font font = new Font(defaultFont.getName(), Font.BOLD,
				defaultFont.getSize());
		GridBagLayout layout = new GridBagLayout();
		this.setLayout(layout);
		String str = ""; //$NON-NLS-1$
        
        //!!!!!!!!!...NOTE...!!!!!!!!!
        //Code in replaceStepNum() depends on "Step " occurring at the start of the string, immediately
        //followed by the step number, which is immediately followed by " (Optional): " or ": ".  Not very 
        //elegant but as usual I am in a hurry.  BWP 09/01/04
        //
		if (stepNum > 0) {
			str += "Step " + stepNum; //$NON-NLS-1$
			if (optional) {
				str += " (Optional)"; //$NON-NLS-1$
			}
			str += ": "; //$NON-NLS-1$
		}
		str += header;
		for (int i = 0; i < paragraphs.length; i++) {
			str += '\n' + paragraphs[i];
		}
		this.setBorder(BorderFactory.createEtchedBorder());
		textPane = new JTextArea();
		textPane.setFont(font);
		textPane.setText(str);
		textPane.setLineWrap(true);
		textPane.setWrapStyleWord(true);
		textPane.setEditable(false);
		textPane.setBackground((new JPanel()).getBackground());
		this.add(textPane);
		this.setBackground(textPane.getBackground());
		layout.setConstraints(textPane, new GridBagConstraints(1, 1, 1, 1,
				1.0, 1.0, GridBagConstraints.CENTER, 
				GridBagConstraints.BOTH, new Insets(3, 3, 3, 3), 0, 0));
	}
	
	public Dimension getMinimumSize() {
		Dimension superMinSize = super.getMinimumSize();
		Dimension ourMinSize = new Dimension(superMinSize.width,
				Math.max(superMinSize.height, MINIMUM_HEIGHT));
		return ourMinSize;
	}
    
    public int getStepNum() {
        return stepNum;
    }
    
    public void replaceStepNum(int newStepNum) {
        String text = textPane.getText();
        int blankLoc = text.indexOf(' ', 5);
        int colonLoc = text.indexOf(':', 5);
        //Note that according to the syntax in init(), both a blank and a colon must occur, so the 
        //following code is safe.
        int nextNonNumericLoc = Math.min(blankLoc, colonLoc);
        String newStepNumStr = (new Integer(newStepNum)).toString();
        String newText = text.substring(0, 5) + newStepNumStr + text.substring(nextNonNumericLoc);
        textPane.setText(newText);
    }
    
    public String getText() {
        return textPane.getText();
    }
}
