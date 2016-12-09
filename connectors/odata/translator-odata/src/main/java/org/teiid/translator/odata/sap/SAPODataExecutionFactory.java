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
package org.teiid.translator.odata.sap;

import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.Translator;
import org.teiid.translator.WSConnection;
import org.teiid.translator.odata.ODataExecutionFactory;

@Translator(name="sap-gateway", description="A translator for making OData data service calls to SAP Gateway")
public class SAPODataExecutionFactory extends ODataExecutionFactory {

	public SAPODataExecutionFactory() {
		super();
		setSupportsOdataCount(true);
		setSupportsOdataFilter(true);
		setSupportsOdataOrderBy(true);
		setSupportsOdataSkip(true);
		setSupportsOdataTop(true);
	}

	@Override
	public MetadataProcessor<WSConnection> getMetadataProcessor() {
		return new SAPMetadataProcessor();
	}
}
