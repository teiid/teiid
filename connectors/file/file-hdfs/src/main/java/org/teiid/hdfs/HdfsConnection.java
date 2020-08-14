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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
        Path path = new Path(location);
        try {
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            if(fileStatus.isDirectory()){
                return convert(path);
            }
            if(fileStatus.isFile()) {
                return convert(Arrays.asList(fileStatus));
            }
        } catch (FileNotFoundException e){
            //ignore and see if it's a wildcard
        } catch (IOException e) {
            throw new TranslatorException(e);
        }

        if(location.contains("*")){ //$NON-NLS-1$
            location = location.replaceAll("\\\\", "\\\\\\\\"); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\?", "\\\\?"); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\[", "\\\\["); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\{", "\\\\{"); //$NON-NLS-1$ //$NON-NLS-2$

            try {
                FileStatus[] status = fileSystem.globStatus(new Path(location));
                return convert(Arrays.asList(status));
            } catch (IOException e) {
                throw new TranslatorException(e);
            }
        }
        return null;
    }

    private VirtualFile[] convert(Path path) throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(path,false);
        List<FileStatus> statuses = new ArrayList<>();
        while(fileStatusRemoteIterator.hasNext()){
            statuses.add(fileStatusRemoteIterator.next());
        }
        return convert(statuses);
    }

    private VirtualFile[] convert(List<FileStatus> statuses) {
        VirtualFile[] result = new VirtualFile[statuses.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = new HdfsVirtualFile(fileSystem, statuses.get(i));
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
