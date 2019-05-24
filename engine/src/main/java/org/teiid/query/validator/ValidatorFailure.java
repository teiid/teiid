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

package org.teiid.query.validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.teiid.query.report.ReportItem;
import org.teiid.query.sql.LanguageObject;

public class ValidatorFailure extends ReportItem {

    public enum Status {
        ERROR,
        WARNING
    }

    public static final String VALIDATOR_FAILURE = "ValidatorFailure"; //$NON-NLS-1$

    // Don't want to pass this around, so make it transient
    private transient Collection<LanguageObject> invalidObjects;
    private Status status = Status.ERROR;

    public ValidatorFailure(String description) {
        super(VALIDATOR_FAILURE);
        setMessage(description);
        this.invalidObjects = Collections.emptyList();
    }

    public ValidatorFailure(String description, LanguageObject object) {
        super(VALIDATOR_FAILURE);
        setMessage(description);
        this.invalidObjects = new ArrayList<LanguageObject>(1);
        this.invalidObjects.add(object);
    }

    public ValidatorFailure(String description, Collection<? extends LanguageObject> objects) {
        super(VALIDATOR_FAILURE);
        setMessage(description);
        this.invalidObjects = new ArrayList<LanguageObject>(objects);
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    /**
     * Get count of invalid objects.
     * @return Count of invalid objects
     */
    public int getInvalidObjectCount() {
        if(this.invalidObjects == null) {
            return 0;
        }
        return this.invalidObjects.size();
    }

    /**
     * Get the objects that failed validation.
     * @return Invalid objects, may be null
     */
    public Collection<LanguageObject> getInvalidObjects() {
        return this.invalidObjects;
    }

    /**
     * Return description
     * @return Description of failure
     */
    public String toString() {
        return getMessage();
    }

}
