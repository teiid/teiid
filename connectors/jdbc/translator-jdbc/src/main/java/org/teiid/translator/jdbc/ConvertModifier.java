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

package org.teiid.translator.jdbc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;

/**
 * Base class for handling the convert function.
 * <p>Convert is by far the most complicated pushdown function since it actually
 * represents a matrix of possible functions. Additionally not every source supports
 * the same semantics as our conversions.
 * <p>Each instance of this class makes a best effort at handling converts for for a
 * given source - compensating for differing string representation, the lack a time type
 * etc.
 * <p>The choice of conversion logic is as follows:
 * <ul>
 *  <li>Provide specific conversion between the source and target - {@link #addConvert(int, int, FunctionModifier)}
 *  mostly one do not need to provide any conversion if the default is cast(srcType AS targetType), however if the source
 *  database requires different specific format for example to cast(srctype, targetType FORMAT 'more-info') this conversion needs to be added</li>
 *  <li>Filter common implicit conversions</li>
 *  <li>Look for a general source conversion - {@link #addSourceConversion(FunctionModifier, int...)}</li>
 *  <li>Look for a general target conversion - {@link #addTypeConversion(FunctionModifier, int...)}. If the source
 *  database provides a specific function for converting *any* source datatype to this target datatype then use this to define it.
 *  Like in oracle "to_char" function will convert any other data type to string. Use this to define those kind of conversions
 *  convert any data type to string. so you can use this for purpose.</li>
 *  <li>Type maps from database data type to Teiid runtime types. - {@link #addTypeMapping(String, int...)}
 *  define mappings for every datatype available in database. The cast operation will replace the target type (teiid type) with the given
 *  native type in the cast operation generated. Do not need to really look at implicit/explicit conversions that are
 *  supported by the source database, because when a cast is defined on the sql it is up to the source database to apply it
 *  as implicit or explicit operation. Teiid generates cast always when needed</li>
 *  <li>Drop the conversion</li>
 * </ul>
 */
public class ConvertModifier extends FunctionModifier {

    public static class FormatModifier extends AliasModifier {

        private String format;

        public FormatModifier(String alias) {
            super(alias);
        }

        public FormatModifier(String alias, String format) {
            super(alias);
            this.format = format;
        }

        @Override
        public List<?> translate(Function function) {
            modify(function);
            if (format == null) {
                function.getParameters().remove(1);
            } else {
                ((Literal)function.getParameters().get(1)).setValue(format);
            }
            return null;
        }

    }

    private Map<Integer, String> typeMapping = new HashMap<Integer, String>();
    private Map<Integer, FunctionModifier> typeModifier = new HashMap<Integer, FunctionModifier>();
    private Map<Integer, FunctionModifier> sourceModifier = new HashMap<Integer, FunctionModifier>();
    private Map<List<Integer>, FunctionModifier> specificConverts = new HashMap<List<Integer>, FunctionModifier>();
    private boolean booleanNumeric;
    private boolean wideningNumericImplicit;
    private boolean booleanNullable = true;

    public void addTypeConversion(FunctionModifier convert, int ... targetType) {
        for (int i : targetType) {
            this.typeModifier.put(i, convert);
        }
    }

    public void addSourceConversion(FunctionModifier convert, int ... sourceType) {
        for (int i : sourceType) {
            this.sourceModifier.put(i, convert);
        }
    }

    public void addTypeMapping(String nativeType, int ... targetType) {
        for (int i : targetType) {
            typeMapping.put(i, nativeType);
        }
    }

    public void setWideningNumericImplicit(boolean wideningNumericImplicit) {
        this.wideningNumericImplicit = wideningNumericImplicit;
    }

    public void addConvert(int sourceType, int targetType, FunctionModifier convert) {
        specificConverts.put(Arrays.asList(sourceType, targetType), convert);
    }

    @Override
    public List<?> translate(Function function) {
        function.setName("cast"); //$NON-NLS-1$
        int targetCode = getCode(function.getType());
        List<Expression> args = function.getParameters();
        Class<?> srcType = args.get(0).getType();
        int sourceCode = getCode(srcType);

        List<Integer> convesionCode = Arrays.asList(sourceCode, targetCode);
        FunctionModifier convert = specificConverts.get(convesionCode);
        if (convert != null) {
            return convert.translate(function);
        }

        boolean implicit = sourceCode == CHAR && targetCode == STRING;

        if (targetCode >= BYTE && targetCode <= BIGDECIMAL) {
            if (booleanNumeric && sourceCode == BOOLEAN) {
                sourceCode = BYTE;
                implicit = targetCode == BYTE;
            }
            implicit |= wideningNumericImplicit && sourceCode >= BYTE && sourceCode <= BIGDECIMAL && sourceCode < targetCode;
        }

        if (!implicit) {
            convert = this.sourceModifier.get(sourceCode);
            if (convert != null
                     && (!convert.equals(sourceModifier.get(targetCode)) || sourceCode == targetCode)) { //checks for implicit, but allows for dummy converts
                return convert.translate(function);
            }

            convert = this.typeModifier.get(targetCode);
            if (convert != null
                     && (!convert.equals(typeModifier.get(sourceCode)) || sourceCode == targetCode)) { //checks for implicit, but allows for dummy converts
                return convert.translate(function);
            }

            String type = typeMapping.get(targetCode);

            if (type != null
                    && (!type.equals(typeMapping.get(sourceCode)) || sourceCode == targetCode)) { //checks for implicit, but allows for dummy converts
                ((Literal)function.getParameters().get(1)).setValue(type);
                return null;
            }
        }

        return Arrays.asList(function.getParameters().get(0));
    }

    /**
     * IMPORTANT: only for use with default runtime type names
     * @param langFactory
     * @param expr
     * @param typeName
     * @return
     */
    public static Function createConvertFunction(LanguageFactory langFactory, Expression expr, String typeName) {
        Class<?> type = TypeFacility.getDataTypeClass(typeName);
        return langFactory.createFunction(SourceSystemFunctions.CONVERT,
                new Expression[] {expr, langFactory.createLiteral(typeName, type)}, type);
    }

    public void addNumericBooleanConversions() {
        this.booleanNumeric = true;
        //number -> boolean
        this.addTypeConversion(new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                Expression stringValue = function.getParameters().get(0);
                return Arrays.asList("CASE WHEN ", stringValue, " = 0 THEN 0 WHEN ", stringValue, " IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }, FunctionModifier.BOOLEAN);
        this.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                Expression booleanValue = function.getParameters().get(0);
                if (!booleanNullable) {
                    return Arrays.asList("CASE WHEN ", booleanValue, " = 0 THEN 'false' ELSE 'true' END"); //$NON-NLS-1$ //$NON-NLS-2$
                }
                return Arrays.asList("CASE WHEN ", booleanValue, " = 0 THEN 'false' WHEN ", booleanValue, " IS NOT NULL THEN 'true' END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
        this.addConvert(FunctionModifier.STRING, FunctionModifier.BOOLEAN, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                Expression stringValue = function.getParameters().get(0);
                return Arrays.asList("CASE WHEN ", stringValue, " IN ('false', '0') THEN 0 WHEN ", stringValue, " IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        });
    }

    public void setBooleanNullable(boolean booleanNullable) {
        this.booleanNullable = booleanNullable;
    }

    /**
     * Return true if there is a type mapping or simple modifier for the given type
     * @param type
     * @return
     */
    public boolean hasTypeMapping(int type) {
        return this.typeMapping.containsKey(type) || this.typeModifier.containsKey(type);
    }

    /**
     * Return the direct type mapping for a given type code
     * @param code
     * @return
     */
    public String getSimpleTypeMapping(int code) {
        return typeMapping.get(code);
    }

}
