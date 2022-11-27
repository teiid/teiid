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
package org.teiid.translator;

import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;

/**
 * Marker interface to define metadata for a translator
 */
public interface MetadataProcessor<C> {

    static final String SOURCE_PREFIX = AbstractMetadataRecord.RELATIONAL_PREFIX+"source_"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable= {Table.class, Procedure.class}, datatype=String.class,
            display="Fully qualified name of the source object.  The format is / separated name value pairs that represent the object path in the metadata tree."
                    + " Each name and value are separated by = and are both URL encoded.  Each name will be lower case and is source dependent. e.g. schema=s/table=t")
    static final String FQN = AbstractMetadataRecord.RELATIONAL_PREFIX+"fqn"; //$NON-NLS-1$

    public void process(MetadataFactory metadataFactory, C connection) throws TranslatorException;
}