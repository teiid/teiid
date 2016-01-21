/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.olingo.commons.core.edm.primitivetype;

import java.util.regex.Pattern;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.core.Decoder;
import org.apache.olingo.commons.core.Encoder;

/**
 * REMOVE AFTER Olingo 4.2.0 Update. 
 * has changes from https://issues.apache.org/jira/browse/OLINGO-853
 * DO NOT EDIT unless the changes are commited upstream
 */
public final class EdmString extends SingletonPrimitiveType {

  private static final Pattern PATTERN_ASCII = Pattern.compile("\\p{ASCII}*");

  private static final EdmString INSTANCE = new EdmString();

  {
    uriPrefix = "'";
    uriSuffix = "'";
  }

  public static EdmString getInstance() {
    return INSTANCE;
  }

  @Override
  public Class<?> getDefaultType() {
    return String.class;
  }

  @Override
  protected <T> T internalValueOfString(final String value,
      final Boolean isNullable, final Integer maxLength, final Integer precision,
      final Integer scale, final Boolean isUnicode, final Class<T> returnType) throws EdmPrimitiveTypeException {

    if (isUnicode != null && !isUnicode && !PATTERN_ASCII.matcher(value).matches()
        || maxLength != null && maxLength < value.length()) {
      throw new EdmPrimitiveTypeException("The literal '" + value + "' does not match the facets' constraints.");
    }

    if (returnType.isAssignableFrom(String.class)) {
      return returnType.cast(value);
    } else {
      throw new EdmPrimitiveTypeException("The value type " + returnType + " is not supported.");
    }
  }

  @Override
  protected <T> String internalValueToString(final T value,
      final Boolean isNullable, final Integer maxLength, final Integer precision,
      final Integer scale, final Boolean isUnicode) throws EdmPrimitiveTypeException {

    final String result = value instanceof String ? (String) value : String.valueOf(value);

    if (isUnicode != null && !isUnicode && !PATTERN_ASCII.matcher(result).matches()
        || maxLength != null && maxLength < result.length()) {
      throw new EdmPrimitiveTypeException("The value '" + value + "' does not match the facets' constraints.");
    }

    return result;
  }

  @Override
  public String toUriLiteral(final String literal) {
    if (literal == null) {
      return null;
    }

    final int length = literal.length();

    final StringBuilder uriLiteral = new StringBuilder(length + 2);
    uriLiteral.append(uriPrefix);
    for (int i = 0; i < length; i++) {
      final char c = literal.charAt(i);
      if (c == '\'') {
        uriLiteral.append(c);
      }
      uriLiteral.append(c);
    }
    uriLiteral.append(uriSuffix);
    return Encoder.encode(uriLiteral.toString());
  }

  @Override
  public String fromUriLiteral(final String literal) throws EdmPrimitiveTypeException {
    return literal == null ? null : Decoder.decode(super.fromUriLiteral(literal).replace("''", "'"));
  }
}
