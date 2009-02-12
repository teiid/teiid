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

package com.metamatrix.console.ui.views.entitlements;

import java.awt.Color;

import javax.swing.BorderFactory;

import com.metamatrix.toolbox.ui.widget.CheckBox;

public class AuthorizationsCheckBox extends CheckBox {
	private int currentRow = -1;
	private int currentColumn = -1;
	private String useType = null; //editor or renderer
	
    public AuthorizationsCheckBox() {
        super();
        this.setBorderPaintedFlat(true);
        this.setHorizontalAlignment(CENTER);
        this.setBorder(BorderFactory.createLineBorder(Color.black));
    }
    
    public int getCurrentRow() {
    	return currentRow;
    }
    
    public int getCurrentColumn() {
    	return currentColumn;
    }
    
    public void setCurrentRow(int row) {
    	currentRow = row;
    }
    
    public void setCurrentColumn(int column) {
    	currentColumn = column;
    }
    
    public String getUseType() {
    	return useType;
    }
    
    public void setUseType(String type) {
    	useType = type;
    }
    
    public String toString() {
    	String str = "AuthorizationsCheckBox: currentRow=" + currentRow +
    			",currentColumn=" + currentColumn + ",useType=" + useType +
    			",isEnabled()=" + this.isEnabled();
    	return str;
    }
}
