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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;

/**
 * Contains TEIID-3938, remove after upgrading to Olingo 4.2.0
 */
public final class EdmTimeOfDay extends SingletonPrimitiveType {

  private static final Pattern PATTERN = Pattern.compile(
      "(\\p{Digit}{2}):(\\p{Digit}{2})(?::(\\p{Digit}{2})(\\.(\\p{Digit}{0,}?)0*)?)?");

  private static final EdmTimeOfDay INSTANCE = new EdmTimeOfDay();

  public static EdmTimeOfDay getInstance() {
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

    final Matcher matcher = PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new EdmPrimitiveTypeException("The literal '" + value + "' has illegal content.");
    }

    final Calendar dateTimeValue = Calendar.getInstance(EdmDateTimeOffset.getDefaultTimeZone());
    dateTimeValue.clear();
    dateTimeValue.set(Calendar.HOUR_OF_DAY, Byte.parseByte(matcher.group(1)));
    dateTimeValue.set(Calendar.MINUTE, Byte.parseByte(matcher.group(2)));
    dateTimeValue.set(Calendar.SECOND, matcher.group(3) == null ? 0 : Byte.parseByte(matcher.group(3)));

    int nanoSeconds = 0;
    if (matcher.group(4) != null) {
      if (matcher.group(4).length() == 1 || matcher.group(4).length() > 13) {
        throw new EdmPrimitiveTypeException("The literal '" + value + "' has illegal content.");
      }
      final String decimals = matcher.group(5);
      if (decimals.length() > (precision == null ? 0 : precision)) {
        throw new EdmPrimitiveTypeException("The literal '" + value + "' does not match the facets' constraints.");
      }
      final String milliSeconds = decimals.length() > 3 ?
          decimals.substring(0, 3) :
            decimals + "000".substring(decimals.length());
          final short millis = Short.parseShort(milliSeconds);
          if (returnType.isAssignableFrom(Timestamp.class)) {
            nanoSeconds = millis * 1000 * 1000;
          } else {
            dateTimeValue.set(Calendar.MILLISECOND, millis);
          }
    }

    try {
      return EdmDateTimeOffset.convertDateTime(dateTimeValue, nanoSeconds, returnType);
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
    
    final StringBuilder result = new StringBuilder();
    EdmDateTimeOffset.appendTwoDigits(result, dateTimeValue.get(Calendar.HOUR_OF_DAY));
    result.append(':');
    EdmDateTimeOffset.appendTwoDigits(result, dateTimeValue.get(Calendar.MINUTE));
    result.append(':');
    EdmDateTimeOffset.appendTwoDigits(result, dateTimeValue.get(Calendar.SECOND));

    try {
      EdmDateTimeOffset.appendFractionalSeconds(result,
          dateTimeValue.get(Calendar.MILLISECOND), value instanceof Timestamp, precision);
    } catch (final IllegalArgumentException e) {
      throw new EdmPrimitiveTypeException("The value '" + value + "' does not match the facets' constraints.", e);
    }

    return result.toString();
  }
}
