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

package com.metamatrix.console.ui.views.deploy.util;

import com.metamatrix.toolbox.ui.Validator;

public final class TextValidator implements Validator {
    private int minLength = 0;
    private int maxLength = 0;
    private boolean required = false;

    public TextValidator(int theMinLength,
                         int theMaxLength,
                         boolean theRequiredFlag) {
        minLength = theMinLength;
        maxLength = theMaxLength;
        required = theRequiredFlag;
    }

    public Object validate(final Object theObject) {
        if (!(theObject instanceof String)) {
            throw new IllegalArgumentException(
                "Object is not a String. Class=" + theObject); //$NON-NLS-1$
        }

        String text = (String)theObject;
        int length = (text == null) ? 0 : text.length();

        boolean result = true;
        if (required && (length == 0)) {
            result = false;
        }
        else {
            if (!required && (length == 0)) {
                result = true;
            }
            else if ((length < minLength) || (length > maxLength)) {
                result = false;
            }
        }
        return new Boolean(result);
    }

}
