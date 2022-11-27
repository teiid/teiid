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

package org.teiid.query.function.metadata;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.FunctionMetadataException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.validator.ValidatorReport;


/**
 * The validator encodes business logic with respect to what a valid function
 * description is.  These methods call each other from the most complex components
 * (FunctionMethod) to the simplest pieces (function name).  Certain users
 * of the validator may only need to call lower level methods.
 */
public class FunctionMetadataValidator {

    /**
     *  Maximum length for function names, parameter names, categories, and descriptions.
     */
    public static final int MAX_LENGTH = 255;

    // Can't construct
    private FunctionMetadataValidator() {
    }

    /**
     * Validate a collection of {@link FunctionMethod} objects.
     * @param methods Collection of {@link FunctionMethod} objects
     * @param report Report to store validation errors
     */
    public static final void validateFunctionMethods(Collection<FunctionMethod> methods, ValidatorReport report) {
        validateFunctionMethods(methods, report, null);
    }

    public static final void validateFunctionMethods(Collection<FunctionMethod> methods, ValidatorReport report, Map<String, Datatype> runtimeTypeMap) {
        if (runtimeTypeMap == null) {
            runtimeTypeMap = SystemMetadata.getInstance().getRuntimeTypeMap();
        }
        if(methods != null) {
            for (FunctionMethod method : methods) {
                validateFunctionMethod(method, report, runtimeTypeMap);
            }
        }
    }

    /**
     * Determine whether a FunctionMethod is valid.  The following items are validated:
     * <UL>
     * <LI>Validate method name</LI>
     * <LI>Validate description</LI>
     * <LI>Validate category</LI>
     * <LI>Validate invocation method</LI>
     * <LI>Validate all input parameters</LI>
     * <LI>Validate output parameter</LI>
     * </UL>
     * @param method The method to validate
     * @param report The report to update during validation
     */
    public static final void validateFunctionMethod(FunctionMethod method, ValidatorReport report, Map<String, Datatype> runtimeTypeMap) {
        if(method == null) {
            updateReport(report, method, QueryPlugin.Util.getString("ERR.015.001.0052", "FunctionMethod")); //$NON-NLS-1$ //$NON-NLS-2$
            return;  // can't validate
        }

        try {
            // Validate attributes
            validateName(method.getName());
            validateDescription(method.getDescription());
            validateCategory(method.getCategory());
            validateInvocationMethod(method.getInvocationClass(), method.getInvocationMethod(), method.getPushdown());
            // Validate input parameters
           List<FunctionParameter> params = method.getInputParameters();
            if(params != null && !params.isEmpty()) {
                for(int i=0; i<params.size(); i++) {
                    FunctionParameter param = params.get(i);
                    validateFunctionParameter(param);
                    param.setPosition(i+1);
                    MetadataFactory.setDataType(param.getRuntimeType(), param, runtimeTypeMap, true);
                    param.getUUID();
                }
            }

            // Validate output parameters
            validateFunctionParameter(method.getOutputParameter());
            method.getOutputParameter().setPosition(0);
            MetadataFactory.setDataType(method.getOutputParameter().getRuntimeType(), method.getOutputParameter(), runtimeTypeMap, true);
        } catch(FunctionMetadataException e) {
            updateReport(report, method, e.getMessage());
        }
    }

    /**
     * Update a report with a validation error.
     * @param report The report to update
     * @param method The function method
     * @param message The message about the validation failure
     */
    private static final void updateReport(ValidatorReport report, FunctionMethod method, String message) {
        report.addItem(new InvalidFunctionItem(method, message));
    }

    /**
     * Determine whether a FunctionParameter is valid.  The following items are validated:
     * <UL>
     * <LI>Validate parameter name</LI>
     * <LI>Validate description</LI>
     * <LI>Validate type</LI>
     * </UL>
     * @param param The parameter to validate
     * @throws FunctionMetadataException Thrown if function parameter is not valid in some way
     */
    public static final void validateFunctionParameter(FunctionParameter param) throws FunctionMetadataException {
        if(param == null) {
             throw new FunctionMetadataException(QueryPlugin.Event.TEIID30427, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30427));
        }

        // Validate attributes
        validateName(param.getName());
        validateDescription(param.getDescription());
    }

    /**
     * Determine whether a function or parameter name is valid.  The following items are validated:
     * <UL>
     * <LI>Validate that name is not null</LI>
     * <LI>Validate that name has length &lt;= MAX_LENGTH</LI>
     * <LI>Validate that name starts with alphabetic character</LI>
     * <LI>Validate that name contains only valid characters: letters, numbers, and _</LI>
     * </UL>
     * @param name Name to validate
     * @throws FunctionMetadataException Thrown if function or parameter name is not valid in some way
     */
    public static final void validateName(String name) throws FunctionMetadataException {
        validateIsNotNull(name, "Name"); //$NON-NLS-1$
        validateLength(name, MAX_LENGTH, "Name"); //$NON-NLS-1$
    }

    /**
     * Determine whether a description is valid.  The following items are validated:
     * <UL>
     * <LI>Validate that description (if not null) has length &lt;= 4000</LI>
     * </UL>
     * @param description Description to validate
     * @throws FunctionMetadataException Thrown if description is not valid in some way
     */
    public static final void validateDescription(String description) throws FunctionMetadataException {
        if(description != null) {
            validateLength(description, DataTypeManager.MAX_STRING_LENGTH, "Description"); //$NON-NLS-1$
        }
    }

    /**
     * Determine whether a category is valid.  The following items are validated:
     * <UL>
     * <LI>Validate that category is not null</LI>
     * <LI>Validate that category has length &lt;= MAX_LENGTH</LI>
     * </UL>
     * @param category Category to validate
     * @throws FunctionMetadataException Thrown if category is not valid in some way
     */
    public static final void validateCategory(String category) throws FunctionMetadataException {
        if (category != null) {
            validateLength(category, MAX_LENGTH, "Category"); //$NON-NLS-1$
        }
    }

    /**
     * Determine whether an invocation class and method are valid.  The following items are validated:
     * <UL>
     * <LI>Validate that invocation class is not null</LI>
     * <LI>Validate that invocation method is not null</LI>
     * <LI>Validate that class is valid Java class name</LI>
     * <LI>Validate that method is valid Java method name</LI>
     * </UL>
     * @param invocationClass Invocation class to validate
     * @param invocationMethod Invocation method to validate
     * @throws FunctionMetadataException Thrown if invocation method is not valid in some way
     */
    public static final void validateInvocationMethod(String invocationClass, String invocationMethod, PushDown pushdown) throws FunctionMetadataException {
        if (pushdown == PushDown.CAN_PUSHDOWN || pushdown == PushDown.CANNOT_PUSHDOWN) {
            validateIsNotNull(invocationClass, "Invocation class"); //$NON-NLS-1$
            validateIsNotNull(invocationMethod, "Invocation method"); //$NON-NLS-1$
            validateJavaIdentifier(invocationClass, "Invocation class", true); //$NON-NLS-1$
            validateJavaIdentifier(invocationMethod, "Invocation method", false); //$NON-NLS-1$
        }
    }

    /**
     * Check that specified object is not null.  If object is null, throw exception using objName.
     * @param object Object to check for null
     * @param objName Object name used when throwing exception
     * @throws FunctionMetadataException Thrown when object == null
     */
    private static final void validateIsNotNull(Object object, String objName) throws FunctionMetadataException {
        if(object == null) {
             throw new FunctionMetadataException(QueryPlugin.Event.TEIID30429, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30429, objName));
        }
    }

    /**
     * Check that specified string is no longer than maxLength.  If string is longer, an exception is thrown
     * using strName.
     * @param string String to check for length
     * @param maxLength Maximum valid length
     * @param strName Name of string to use in exception message
     * @throws FunctionMetadataException Thrown when string.length() > maxLength
     */
    private static final void validateLength(String string, int maxLength, String strName) throws FunctionMetadataException {
         if(string.length() > maxLength) {
               throw new FunctionMetadataException(QueryPlugin.Event.TEIID30430, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30430,strName, new Integer(maxLength)));
         }
    }

    /**
     * Check that specified string is valid Java identifier.  If not, an exception is thrown using
     * strName for the exception message.
     * @param identifier String to check
     * @param strName String to use in exception message
     * @param allowMultiple True if multiple identifiers are allowed, as in a class name
     * @throws FunctionMetadataException Thrown when string is not valid Java identifier
     */
    private static final void validateJavaIdentifier(String identifier, String strName, boolean allowMultiple) throws FunctionMetadataException {
        // First check first character
        if(identifier.length() > 0) {
            char firstChar = identifier.charAt(0);
            if(! Character.isJavaIdentifierStart(firstChar)) {
                  throw new FunctionMetadataException(QueryPlugin.Event.TEIID30432, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30432,strName, new Character(firstChar)));
            }

            // Then check the rest of the characters
            for(int i=1; i<identifier.length(); i++) {
                char ch = identifier.charAt(i);
                if(! Character.isJavaIdentifierPart(ch)) {
                    if(! allowMultiple || ! (ch == '.')) {
                          throw new FunctionMetadataException(QueryPlugin.Event.TEIID30431, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30431,strName, new Character(ch)));
                    }
                }
            }

            if(identifier.charAt(identifier.length()-1) == '.') {
                  throw new FunctionMetadataException(QueryPlugin.Event.TEIID30434, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30434,strName));
            }
        }
    }

}
