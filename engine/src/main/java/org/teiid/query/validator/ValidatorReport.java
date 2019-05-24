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

import java.util.Collection;
import java.util.Iterator;

import org.teiid.query.QueryPlugin;
import org.teiid.query.report.ActivityReport;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.validator.ValidatorFailure.Status;


public class ValidatorReport extends ActivityReport<ValidatorFailure> {

    public static final String VALIDATOR_REPORT = "Validator Report"; //$NON-NLS-1$

    public ValidatorReport() {
        super(VALIDATOR_REPORT);
    }

    public ValidatorReport(String name) {
        super(name);
    }

    public void collectInvalidObjects(Collection<LanguageObject> invalidObjects) {
        for (ValidatorFailure failure : getItems()) {
            if(failure.getInvalidObjectCount() > 0) {
                invalidObjects.addAll(failure.getInvalidObjects());
            }
        }
    }

    public String getFailureMessage() {
        Collection<ValidatorFailure> failures = getItems();
        if(failures.size() == 0) {
            return QueryPlugin.Util.getString("ERR.015.012.0064"); //$NON-NLS-1$
        } else if(failures.size() == 1) {
            return failures.iterator().next().toString();
        } else {
            StringBuffer err = new StringBuffer();
            err.append(QueryPlugin.Util.getString("ERR.015.012.0063")); //$NON-NLS-1$

            Iterator<ValidatorFailure> iter = failures.iterator();
            while(iter.hasNext()) {
                ValidatorFailure next = iter.next();
                if (next.getStatus() != Status.ERROR) {
                    err.append(next.getStatus()).append(" "); //$NON-NLS-1$
                }
                err.append(next.getMessage());
                if (iter.hasNext()) {
                    err.append(", "); //$NON-NLS-1$
                }
            }
            return err.toString();
        }
    }

    public String toString() {
        return this.getFailureMessage();
    }

    public void handleValidationWarning(String message) {
        ValidatorFailure vf = new ValidatorFailure(message);
        vf.setStatus(Status.WARNING);
        this.addItem(vf);
    }

    public void handleValidationError(String message) {
        this.addItem(new ValidatorFailure(message));
    }

    public void handleValidationError(String message, LanguageObject invalidObj) {
        this.addItem(new ValidatorFailure(message, invalidObj));
    }

    public void handleValidationError(String message, Collection<? extends LanguageObject> invalidObjs) {
        this.addItem(new ValidatorFailure(message, invalidObjs));
    }

}
