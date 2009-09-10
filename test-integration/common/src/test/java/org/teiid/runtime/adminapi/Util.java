package org.teiid.runtime.adminapi;

import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class Util {


    /* Utility methods */
    public static Connection getConnection(String mmUrl) throws Exception {

        final String DRIVER_NAME = "org.teiid.jdbc.TeiidDriver"; //$NON-NLS-1$
        final String USER_NAME = "admin";//$NON-NLS-1$
        final String PASSWORD = "teiid";//$NON-NLS-1$

        Connection conn = null;
        try {
            // using the driver

            Class driverClass = Class.forName(DRIVER_NAME);
            if ( driverClass == null) {
                throw new Exception("load class for Driver");//$NON-NLS-1$
            }

            Driver driver = (Driver) driverClass.newInstance();

            DriverManager.registerDriver( driver );
            conn = DriverManager.getConnection(mmUrl, USER_NAME, PASSWORD);
        } catch (Exception e) {
            throw e;
        }

        return conn;
    }

    public static void closeQuietly(Connection conn) {
        try {
            if (conn != null ) try { conn.close(); } catch (Exception e) {}
        } catch (Exception x) {
            // tastes good
        }
    }

    public static void closeQuietly(Statement stmt) {
        try {
            if (stmt != null ) try { stmt.close(); } catch (Exception e) {}
        } catch (Exception x) {
            // tastes good
        }
    }

    public static void closeQuietly(ResultSet rs) {
        try {
            if (rs != null ) try { rs.close(); } catch (Exception e) {}
        } catch (Exception x) {
            // tastes good
        }
    }

    public static void closeQuietly(Connection conn, Statement stmt, ResultSet rs) {
        closeQuietly(conn);
        closeQuietly(stmt);
        closeQuietly(rs);
    }

    public static String getStackTraceAsString(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        StringBuffer error = stringWriter.getBuffer();
        return error.toString();
    }
    
    @SuppressWarnings("deprecation")
	public static char[] getCharacterFile(String file) throws IOException {
        
        URL url = new File(file).toURL();

        InputStream in = url.openStream();
        CharArrayWriter out = new CharArrayWriter(10 * 1024);
        int b = 0;
        while ((b = in.read()) != -1) {
            out.write(b);
        }
        return out.toCharArray();
    }

    @SuppressWarnings("deprecation")
	public static byte[] getBinaryFile(String file) throws IOException {
        
        URL url = new File(file).toURL();

        InputStream in = url.openStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream(10 * 1024);
        int b = 0;
        while ((b = in.read()) != -1) {
            out.write(b);
        }
        return out.toByteArray();
    }

    public static  void writeToFile(String name, byte[] contents) throws Exception {
        FileOutputStream f = new FileOutputStream(name);
        f.write(contents);
        f.close();
    }
    
    public static void writeToFile(String name, char[] contents)  throws Exception {
        FileWriter f = new FileWriter(name);
        f.write(contents);
        f.close();
    }    
}
