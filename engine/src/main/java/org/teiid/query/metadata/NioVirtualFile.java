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
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * TODO: combine with the file api virtual file - however it's based upon just the File api
 */
public class NioVirtualFile implements VirtualFile {
    private Path path;
    private long size = -1;

    public NioVirtualFile(Path path) {
        this.path = path;
    }

    @Override
    public InputStream openStream() throws IOException {
        FileChannel channel = FileChannel.open(path);
        return Channels.newInputStream(channel);
    }

    @Override
    public long getSize() {
        if (size == -1) {
            try (FileChannel channel = FileChannel.open(path);) {
                size = channel.size();
            } catch (IOException e) {
                size = 0;
            }
        }
        return size;
    }

    @Override
    public String getName() {
        return path.getFileName().toString();
    }

    @Override
    public List<VirtualFile> getFileChildrenRecursively() throws IOException {
        if (!Files.isDirectory(this.path)) {
            return Collections.emptyList();
        }
        List<VirtualFile> children = new ArrayList<>();
        //walking using the stream, seems to fail, so we'll collect with the visitor
        Files.walkFileTree(this.path, new FileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file,
                    BasicFileAttributes attrs) throws IOException {
                children.add(new NioVirtualFile(file));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir,
                    BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }

        });
        return children;
    }

    @Override
    public boolean isFile() {
        return Files.isRegularFile(this.path);
    }

    @Override
    public String getPathName() {
        return path.toString();
    }

    @Override
    public VirtualFile getChild(String child) {
        return new NioVirtualFile(this.path.resolve("/" + child)); //$NON-NLS-1$
    }

    @Override
    public boolean exists() {
        return Files.exists(this.path);
    }

    @Override
    public String toString() {
        return getPathName();
    }
}