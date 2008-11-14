/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.jdbc.syntax;

public class FieldType {

  private String fieldName;
  private boolean hasLength = true;
  private int length=-1;

//  private int minLength;
//  private int maxLength;
//  private int defLength;

  public FieldType(String fieldName) {
      this.fieldName = fieldName;
  }


  public FieldType(String fieldName, boolean hasLength) {
      this.hasLength = hasLength;
      this.fieldName = fieldName;
  }

  public FieldType(String fieldName, int length) {
      this.hasLength = true;
      this.length = length;
  }


  public String getFieldName() {
      return fieldName;
  }

  public boolean hasLength() {
      return hasLength;
  }

  public int getLength() {
      return length;
  }

  public FieldType setLimits(int max, int min, int avg) {
//      this.maxLength = max;
//      this.minLength = min;
//      this.defLength = avg;

      return this;
  }



} 
