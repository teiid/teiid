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

package com.metamatrix.query.validator;

import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.report.ActivityReport;
import com.metamatrix.query.util.ErrorMessageKeys;

public class ValidatorReport extends ActivityReport {

	public static final String VALIDATOR_REPORT = "Validator Report"; //$NON-NLS-1$

    public ValidatorReport() {
        super(VALIDATOR_REPORT);
    }

    public void collectInvalidObjects(Collection invalidObjects) {
        Iterator iter = getItemsByType(ValidatorFailure.VALIDATOR_FAILURE).iterator();
        while(iter.hasNext()) {
            ValidatorFailure failure = (ValidatorFailure) iter.next();
            if(failure.getInvalidObjectCount() > 0) {
                invalidObjects.addAll(failure.getInvalidObjects());
            }
        }
    }

    public String getFailureMessage() {
        Collection failures = getItemsByType(ValidatorFailure.VALIDATOR_FAILURE);
        if(failures.size() == 0) {
            return QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0064);
        } else if(failures.size() == 1) {
            return failures.iterator().next().toString();
        } else {
            StringBuffer err = new StringBuffer();
            err.append(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0063));

            Iterator iter = failures.iterator();
            ValidatorFailure failure = (ValidatorFailure) iter.next();
            err.append(failure);

            while(iter.hasNext()) {
                failure = (ValidatorFailure) iter.next();
                err.append(", "); //$NON-NLS-1$
                err.append(failure);
            }
            return err.toString();
        }
    }

    public String toString() {
        return this.getFailureMessage();
    }

}
