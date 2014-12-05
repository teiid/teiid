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

package org.teiid.translator.google.visitor;

import org.teiid.language.Delete;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
/**
 * Translates SQL DELETE commands
 * 
 * @author felias
 *
 */
public class SpreadsheetDeleteVisitor extends SpreadsheetCriteriaVisitor {

	
	public SpreadsheetDeleteVisitor(SpreadsheetInfo info) {
		super(info);
	}

	public void visit(Delete obj) {
		if (obj.getTable().getMetadataObject().getNameInSource() != null) {
			worksheetTitle = obj.getTable().getMetadataObject().getNameInSource();
		}else{
			worksheetTitle = obj.getTable().getName();	
		}
		worksheetKey = info.getWorksheetByName(worksheetTitle).getId();
		if (obj.getWhere() != null) {
			append(obj.getWhere());
			criteriaQuery = buffer.toString();
		} else {
			criteriaQuery = ""; //$NON-NLS-1$
		}
	}

}
