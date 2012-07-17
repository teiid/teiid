/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
                err.append(iter.next());
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
