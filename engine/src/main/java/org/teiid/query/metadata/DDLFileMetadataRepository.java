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
package org.teiid.query.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.VDBResource;
import org.teiid.query.QueryPlugin;
import org.teiid.query.QueryPlugin.Event;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class DDLFileMetadataRepository implements MetadataRepository {

    @Override
    public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory, String text) throws TranslatorException {
        String ddlFile = factory.getModelProperties().getProperty("ddl-file");
        if (ddlFile != null) {
            text = ddlFile;
        }
        if (text != null) {
            VDBResource resource = factory.getVDBResources().get(text);
            if (resource == null) {
                throw new MetadataException(Event.TEIID31137, QueryPlugin.Util.gs(Event.TEIID31137, text));
            }
            InputStream is;
            try {
                is = resource.openStream();
            } catch (IOException e1) {
                throw new MetadataException(e1);
            }
            try {
                //TODO: could allow for a property driven encoding
                factory.parse(new InputStreamReader(is, Charset.forName("UTF-8"))); //$NON-NLS-1$
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                }
            }
        }
    }

}
