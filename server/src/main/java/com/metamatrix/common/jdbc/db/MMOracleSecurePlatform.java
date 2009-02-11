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

package com.metamatrix.common.jdbc.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.util.ErrorMessageKeys;


/**
 */
public class MMOracleSecurePlatform extends OraclePlatform {

    /**
     * 
     */
    public MMOracleSecurePlatform() {
        super();
        usesStreamsForBlobBinding = false;
        usesStreamsForClobBinding = false;
    }
    
        
    public byte[] convertClobToByteArray(ResultSet results, String columnName) throws TransformationException {
        try {
           
            
            String sdata = results.getString(columnName);
            if (sdata ==null) {
                return null;
            }
            
            char[] cdata = sdata.toCharArray();
            
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            
            int l = cdata.length;
            for (int i=0; i<l; i++) {
                
                byte [] b = { (byte)(cdata[i] & 0xff) } ;
                //, (byte)(cdata[i] >> 8 & 0xff) };
                output.write(b,0,1);

            }            
            
            byte[] data = output.toByteArray();
            
            return data;
        } catch (SQLException sqe) {
           
            throw new TransformationException(sqe, ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0002, columnName));

        }

    }  
    
    public void setBlob(PreparedStatement statement, byte[] data, int column) throws SQLException, IOException {

        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        
        statement.setBinaryStream(column, bis, data.length);
    }     
    
    public void setClob(Object sqlObject, InputStream is, String columnName) throws SQLException, IOException {
            PreparedStatement statement = (PreparedStatement) sqlObject;
            
            byte[] buff = new byte[4096];
            int size = 0; 
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try {
                 for (;;)
                 {
                     size = is.read(buff);
                     if (size == -1)
                     {
                         break;
                     }
                     output.write(buff,0,size);
                 }
            } finally {
                output.close();
                
            }
            
            byte[] data = output.toByteArray();
            
            statement.setBytes(4, data);
            
    }        


}
