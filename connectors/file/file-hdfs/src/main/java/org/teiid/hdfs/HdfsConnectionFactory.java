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

package org.teiid.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

public class HdfsConnectionFactory implements Closeable {

    private HdfsConfiguration config;
    private volatile FileSystem fileSystem;

    public HdfsConnectionFactory(HdfsConfiguration config) {
        this.config = config;
        try {
            getFileSystem();
        } catch (TranslatorException e) {
            //attempt to init the filesystem failed, try again with a connection attempt
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Could not make initial connection to hdfs", e); //$NON-NLS-1$
        }
    }

    protected FileSystem createFileSystem(String fsUri, String resourcePath) throws TranslatorException {
        Configuration configuration = new Configuration();
        if(resourcePath != null){
            boolean classpath = false;
            try (InputStream is = HdfsConnection.class.getResourceAsStream(resourcePath)) {
                classpath = is != null;
            } catch (IOException e) {
            }
            if (classpath) {
                configuration.addResource(resourcePath);
            } else {
                configuration.addResource(new Path(resourcePath));
            }
        }
        try {
            return FileSystem.get(new URI(fsUri), configuration);
        } catch (IOException|URISyntaxException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.fileSystem != null) {
            this.fileSystem.close();
        }
    }

    public FileSystem getFileSystem() throws TranslatorException {
        if (this.fileSystem == null) {
            synchronized (this) {
                if (this.fileSystem == null) {
                    this.fileSystem = createFileSystem(config.getFsUri(), config.getResourcePath());
                }
            }
        }
        return fileSystem;
    }

}
