/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.parquet;

import org.teiid.file.VirtualFileConnection;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;

public class ParquetMetadataProcessor implements MetadataProcessor<VirtualFileConnection> {


    @ExtensionMetadataProperty(applicable= Table.class, datatype=String.class, display="Parquet File Name", description="Parquet root location, may be a directory or single file", required=true)
    public static final String LOCATION = MetadataFactory.PARQUET_PREFIX+"LOCATION"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable= Table.class, datatype=String.class, display="Partitioned Columns", description="Partitioning Scheme Name, used to describe the partitioning scheme the parquet files follow", required=false)
    public static final String PARTITIONED_COLUMNS = MetadataFactory.PARQUET_PREFIX+"PARTITIONED_COLUMNS"; //$NON-NLS-1$

    @Override
    public void process(MetadataFactory metadataFactory, VirtualFileConnection connection) throws TranslatorException {

    }
}
