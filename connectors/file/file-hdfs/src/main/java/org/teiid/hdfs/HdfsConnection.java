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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.translator.TranslatorException;

public class HdfsConnection implements VirtualFileConnection {

    private final FileSystem fileSystem;

    public HdfsConnection(HdfsConnectionFactory connectionFactory) throws TranslatorException {
        this.fileSystem = connectionFactory.getFileSystem();
    }

    public HdfsConnection(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public VirtualFile[] getFiles(String location) throws TranslatorException {
        String globLocation = location;
        if (globLocation.endsWith("/")) { //$NON-NLS-1$
            //directory means all files underneath
            globLocation = globLocation + "*"; //$NON-NLS-1$
        }
        //make literal matches to metacharacters
        globLocation = globLocation.replaceAll("[*]{2}", "[*]"); //$NON-NLS-1$ //$NON-NLS-2$
        globLocation = globLocation.replaceAll("[?{\\[]", "\\\\$0"); //$NON-NLS-1$ //$NON-NLS-2$

        try {
            FileStatus[] status = fileSystem.globStatus(new Path(globLocation));
            return convert(status);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    private VirtualFile[] convert(FileStatus[] status) {
        if (status == null) {
            return null;
        }
        VirtualFile[] result = new VirtualFile[status.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new HdfsVirtualFile(fileSystem, status[i]);
        }
        return result;
    }

    @Override
    public void add(InputStream inputStream, String s) throws TranslatorException {
        try {
            OutputStream out = fileSystem.create(new Path(s));
            IOUtils.copyBytes(inputStream, out, 131072);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public boolean remove(String s) throws TranslatorException {
        try {
            return fileSystem.delete(new Path(s), false);
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void close() {
        //the filesystem is shared, don't close
    }

}
