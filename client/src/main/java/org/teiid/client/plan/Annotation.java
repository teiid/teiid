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

package org.teiid.client.plan;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Annotation describing a decision made during query execution.
 */
public class Annotation implements Externalizable {

    public static final String MATERIALIZED_VIEW = "Materialized View"; //$NON-NLS-1$
    public static final String CACHED_PROCEDURE = "Cached Procedure"; //$NON-NLS-1$
    public static final String HINTS = "Hints"; //$NON-NLS-1$
    public static final String RELATIONAL_PLANNER = "Relational Planner"; //$NON-NLS-1$

    public enum Priority {
        LOW,
        MEDIUM,
        HIGH
    }

    private String category;
    private String annotation;
    private String resolution;
    private Priority priority = Priority.LOW;

    public Annotation() {

    }

    public Annotation(String category, String annotation, String resolution, Priority priority) {
        this.category = category;
        this.annotation = annotation;
        this.resolution = resolution;
        this.priority = priority;
    }

    public String getCategory() {
        return this.category;
    }

    public String getAnnotation() {
        return this.annotation;
    }

    public String getResolution() {
        return this.resolution;
    }

    public Priority getPriority() {
        return this.priority;
    }

    public String toString() {
        return getPriority() + " [" + getCategory() +"] "+ getAnnotation() + " - " + getResolution();  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        annotation = (String)in.readObject();
        category = (String)in.readObject();
        resolution = (String)in.readObject();
        priority = Priority.values()[in.readByte()];
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(annotation);
        out.writeObject(category);
        out.writeObject(resolution);
        out.writeByte(priority.ordinal());
    }
}
