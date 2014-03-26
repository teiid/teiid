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
package org.teiid.translator.mongodb;

import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;

public class MongoDBMetadataProcessor implements MetadataProcessor<MongoDBConnection> {
    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Merge Into Table", description="Declare the name of table that this table needs to be merged into. No separate copy maintained")
    public static final String MERGE = MetadataFactory.MONGO_URI+"MERGE"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Embedded Into Table", description="Declare the name of table that this table needs to be embedded into. A separate copy is also maintained")
    public static final String EMBEDDABLE = MetadataFactory.MONGO_URI+"EMBEDDABLE"; //$NON-NLS-1$

    @Override
    public void process(MetadataFactory metadataFactory, MongoDBConnection connection) throws TranslatorException {
        
    }
}
