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
package org.teiid.runtime.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

import javax.xml.stream.XMLStreamException;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Database;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.JBossLogger;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class ConvertVDB {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("usage: CovertVDB /path/to/file-vdb.xml [/path/to/writefile-vdb.ddl]");
            System.exit(0);
        }

        File f = new File(args[0]);
        if (!f.exists()) {
            System.out.println("vdb file " + f.getAbsolutePath() + " does not exist");
            return;
        }

        if (f.getName().toLowerCase().endsWith(".xml")) {
            if (args.length > 1 && args[1] != null) {
                System.out.println("Writing to file " + args[1]);
                ObjectConverterUtil.write(convert(f).getBytes("UTF-8"), new File(args[1]).getAbsolutePath() );
            } else {
                System.out.println(convert(f));
            }
        } else {
            System.out.println("Unknown file type supplied, only .XML based VDBs are supported");
        }
    }

    public static String convert(File f)
            throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, IOException,
            URISyntaxException, MalformedURLException, AdminException, Exception, FileNotFoundException {

        LogManager.setLogListener(new JBossLogger() {
            @Override
            public boolean isEnabled(String context, int level) {
                return false;
            }
        });

        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);

        MyServer es = new MyServer();

        LogManager.setLogListener(new JBossLogger() {
            @Override
            public boolean isEnabled(String context, int level) {
                return false;
            }
        });

        es.start(ec);
        try {
            return es.convertVDB(new FileInputStream(f));
        } finally {
            es.stop();
        }
    }

    private static class MyServer extends EmbeddedServer {
        @Override
        public ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException {
            return new ExecutionFactory<Object, Object>() {

                @Override
                public boolean isSourceRequired() {
                    return false;
                }

                @Override
                public boolean isSourceRequiredForMetadata() {
                    return false;
                }
            };
        };

        String convertVDB(InputStream is) throws Exception{
            byte[] bytes = ObjectConverterUtil.convertToByteArray(is);
            VDBMetaData metadata = null;
            try {
                metadata = VDBMetadataParser.unmarshall(new ByteArrayInputStream(bytes));
            } catch (XMLStreamException e) {
                throw new VirtualDatabaseException(e);
            }
            metadata.setXmlDeployment(true);

            MetadataStore metadataStore = new MetadataStore();
            final LinkedHashMap<String, ModelMetaData> modelMetaDatas = metadata.getModelMetaDatas();
            for (ModelMetaData m:modelMetaDatas.values()) {
                Schema schema = new Schema();
                schema.setName(m.getName());
                schema.setAnnotation(m.getDescription());
                schema.setVisible(m.isVisible());
                schema.setPhysical(m.isSource());
                schema.setProperties(m.getPropertiesMap());
                metadataStore.addSchema(schema);
            }

            //TODO: attempt to parse schemas to obtain at least partial metadata

            Database db = DatabaseUtil.convert(metadata, metadataStore);
            DDLStringVisitor visitor = new DDLStringVisitor(null, null) {

                @Override
                protected void createdSchmea(Schema schema) {
                }

                @Override
                protected void visit(Schema schema) {
                    ModelMetaData m = modelMetaDatas.get(schema.getName());
                    String replace = "";
                    String sourceName = m.getSourceNames().isEmpty()?"":m.getSourceNames().get(0);
                    String schemaName = m.getPropertiesMap().get("importer.schemaPattern");

                    if (m.getSourceMetadataType().isEmpty()) {
                        // nothing defined; so this is NATIVE only
                       if (m.isSource()) {
                           replace = replaceMetadataTag(m, sourceName, schemaName, true);
                       }
                    } else {
                        // may one or more defined
                        for (int i = 0; i < m.getSourceMetadataType().size(); i++) {
                            String type =  m.getSourceMetadataType().get(i);
                            StringTokenizer st = new StringTokenizer(type, ",");
                            while (st.hasMoreTokens()) {
                                type = st.nextToken().trim();
                                if (type.equalsIgnoreCase("NATIVE")) {
                                    replace += replaceMetadataTag(m, sourceName, schemaName, true);
                                } else if (!type.equalsIgnoreCase("DDL")){
                                    replace += replaceMetadataTag(m, type, schemaName, false);
                                } else {
                                    replace += m.getSourceMetadataText().get(i) + "\n"; //$NON-NLS-1$
                                }
                            }
                        }
                    }
                    buffer.append(replace);
                }

            };
            visitor.visit(db);
            return visitor.toString();
        }

        private String replaceMetadataTag(ModelMetaData m, String sourceName, String schemaName, boolean server) {
            String replace = "IMPORT";
            if (schemaName != null) {
                replace += " FOREIGN SCHEMA "+SQLStringVisitor.escapeSinglePart(schemaName);
            }
            replace += " FROM " + (server?"SERVER ":"REPOSITORY ")+SQLStringVisitor.escapeSinglePart(sourceName)+" INTO "+SQLStringVisitor.escapeSinglePart(m.getName());
            if (!m.getPropertiesMap().isEmpty()) {
               replace += " OPTIONS (\n";
               Iterator<String> it = m.getPropertiesMap().keySet().iterator();
               while (it.hasNext()) {
                   String key = it.next();
                   replace += ("\t"+SQLStringVisitor.escapeSinglePart(key) +" '"+StringUtil.replaceAll(m.getPropertiesMap().get(key), "'", "''")+"'");
                   if (it.hasNext()) {
                       replace += ",\n";
                   }
               }
               replace += ")";
            }
            replace+=";\n\n";
            return replace;
        }

        @Override
        protected boolean allowOverrideTranslators() {
            return true;
        }
    };

}
