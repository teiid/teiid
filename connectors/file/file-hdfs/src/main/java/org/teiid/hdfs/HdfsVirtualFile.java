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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.teiid.file.VirtualFile;

public class HdfsVirtualFile implements VirtualFile {

    private final FileSystem fileSystem;
    private final FileStatus fileStatus;

    public HdfsVirtualFile(FileSystem fileSystem, FileStatus fileStatus) {
        this.fileSystem = fileSystem;
        this.fileStatus = fileStatus;
    }

    @Override
    public boolean isDirectory() {
        return fileStatus.isDirectory();
    }

    @Override
    public String getPath() {
        return Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString();
    }

    @Override
    public String getName() {
        return fileStatus.getPath().getName();
    }

    @Override
    public InputStream openInputStream(boolean lock) throws IOException {
        return fileSystem.open(fileStatus.getPath());
    }

    @Override
    public OutputStream openOutputStream(boolean lock) throws IOException {
        return fileSystem.create(fileStatus.getPath());
    }

    @Override
    public long getLastModified() {
        return fileStatus.getModificationTime();
    }

    @Override
    public long getCreationTime() {
        // no option available for creation time and as in most cases files only get created once, thus using the modification time
        return fileStatus.getModificationTime();
    }

    @Override
    public long getSize() {
        return fileStatus.getBlockSize();
    }
}
