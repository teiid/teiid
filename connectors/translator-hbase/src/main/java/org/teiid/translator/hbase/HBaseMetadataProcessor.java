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
package org.teiid.translator.hbase;

import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.HBaseConnection;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;

public class HBaseMetadataProcessor implements MetadataProcessor<HBaseConnection> {
	
	@ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="HBase Table Name", description="HBase Table Name", required=true)
	public static final String TABLE = MetadataFactory.HBASE_URI + "TABLE";

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Column Family and Qualifier", description="Column Family and Column Qualifier, seperated by a dot, for eample, 'customer.city' means cell's family is 'customer', qualifier is 'city'", required=true)    
	public static final String CELL = MetadataFactory.HBASE_URI + "CELL";


	@Override
	public void process(MetadataFactory metadataFactory, HBaseConnection connection) throws TranslatorException {
		
	}

}
