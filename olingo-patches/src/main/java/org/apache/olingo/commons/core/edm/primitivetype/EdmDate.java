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

import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;

/**
 * Implementation of the EDM primitive type Date.
 * Contains TEIID-3938, remove after upgrading to Olingo 4.2.0
 */
public final class EdmDate extends SingletonPrimitiveType {

  private static final Pattern PATTERN = Pattern.compile("(-?\\p{Digit}{4,})-(\\p{Digit}{2})-(\\p{Digit}{2})");

  private static final EdmDate INSTANCE = new EdmDate();

  public static EdmDate getInstance() {
    return INSTANCE;
  }

  @Override
  public Class<?> getDefaultType() {
    return Calendar.class;
  }

  @Override
  protected <T> T internalValueOfString(final String value,
      final Boolean isNullable, final Integer maxLength, final Integer precision,
      final Integer scale, final Boolean isUnicode, final Class<T> returnType) throws EdmPrimitiveTypeException {

    final Calendar dateTimeValue = Calendar.getInstance(EdmDateTimeOffset.getDefaultTimeZone());
    dateTimeValue.clear();

    final Matcher matcher = PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new EdmPrimitiveTypeException("The literal '" + value + "' has illegal content.");
    }

    dateTimeValue.set(
        Integer.parseInt(matcher.group(1)),
        Byte.parseByte(matcher.group(2)) - 1, // month is zero-based
        Byte.parseByte(matcher.group(3)));

    try {
      return EdmDateTimeOffset.convertDateTime(dateTimeValue, 0, returnType);
    } catch (final IllegalArgumentException e) {
      throw new EdmPrimitiveTypeException("The literal '" + value + "' has illegal content.", e);
    } catch (final ClassCastException e) {
      throw new EdmPrimitiveTypeException("The value type " + returnType + " is not supported.", e);
    }
  }

  @Override
  protected <T> String internalValueToString(final T value,
      final Boolean isNullable, final Integer maxLength, final Integer precision,
      final Integer scale, final Boolean isUnicode) throws EdmPrimitiveTypeException {

    final Calendar dateTimeValue = EdmDateTimeOffset.createDateTime(value, true);

    final StringBuilder result = new StringBuilder(10); // Ten characters are enough for "normal" dates.
    final int year = dateTimeValue.get(Calendar.YEAR);
    if (year < 0 || year >= 10000) {
      result.append(year);
    } else {
      EdmDateTimeOffset.appendTwoDigits(result, (year / 100) % 100);
      EdmDateTimeOffset.appendTwoDigits(result, year % 100);
    }
    result.append('-');
    EdmDateTimeOffset.appendTwoDigits(result, dateTimeValue.get(Calendar.MONTH) + 1); // month is zero-based
    result.append('-');
    EdmDateTimeOffset.appendTwoDigits(result, dateTimeValue.get(Calendar.DAY_OF_MONTH));
    return result.toString();
  }
}
