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

package com.metamatrix.connector.sysadmin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.metamatrix.admin.api.server.AdminRoles;


/** 
 * @since 4.3
 */
public class MethodTests implements ITestMethods{
    
    private static FakeObject TESTOBJECT = new FakeObject();
    
    MethodTests() {
    }

    public void getHosts() {
  }
  public void getConnectorBindings(String arg) {
  }
  public void setUserPassword(int arg) {
  }          
  // return 3 rows with 3 columns
  public Collection getConnectorBindings(String arg1, int arg2) {
      List r = new ArrayList(3);
      for (int i=1; i < 4; i++) {
          List c = new ArrayList(3);
          c.add("r" + i + "c1"); //$NON-NLS-1$ //$NON-NLS-2$
          c.add(new Integer(2)); 
          c.add("r" + i + "c3"); //$NON-NLS-1$ //$NON-NLS-2$
          r.add(c);
      }

      return r;
  }
  
  // return 3 rows with 3 columns          
  public Collection getUsers() {
      List r = new ArrayList(3);
      for (int i=1; i < 4; i++) {
          List c = new ArrayList(3);
          c.add("r" + i + "c1"); //$NON-NLS-1$ //$NON-NLS-2$
          c.add(new Integer(2)); 
          c.add("r" + i + "c3"); //$NON-NLS-1$ //$NON-NLS-2$
          r.add(c);
      }
      return r;
  }
  
  public Date getCaches(int arg) {
      return new Date(); 
      //TESTOBJECT;
  }
  
  public Collection getGroups(int arg1, int arg2) {
      List r = new ArrayList(2);
      r.add(TESTOBJECT);
      r.add(TESTOBJECT);
      return r;
  }          

  public Collection fakeMethod(int arg1, int arg2) {
      List r = new ArrayList(2);
      r.add(TESTOBJECT);
      r.add(TESTOBJECT);
      return r; 
  }
  
  public void addUser(String userName, String userPassword) {
      System.out.print("Added user: " + userName + " with Password " + userPassword);//$NON-NLS-1$ //$NON-NLS-2$
  }

  public void setUserPassword(String userName, String userPassword) {
       System.out.println("Password for user" + userName + " was changed to " + userPassword);   //$NON-NLS-1$ //$NON-NLS-2$

  }

public String getUserName() {
	// TODO Auto-generated method stub
	return null;
}
}
