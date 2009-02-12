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

package com.metamatrix.common.tree.directory;

import java.io.File;
import java.io.PrintStream;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.OSPlatformUtil;
import com.metamatrix.core.util.Assertion;

/**
 * PlatformAwareFileSystemView
 */
public class PlatformAwareFilePermission {
    private static String SEPARATOR = File.separator;


    /** The operating system on which MetaMatrix runs*/
    private static int operatingSystem = OSPlatformUtil.getOperatingSystem();
    
   /** Get the operating system on which the IDE is running.
    * @return one of the <code>OS_*</code> constants (such as {@link #OS_WINNT})
    */
    public static final int getOperatingSystem () {
        return operatingSystem;
    }

   /** Test whether the IDE is running on some variant of Windows.
    * @return <code>true</code> if Windows, <code>false</code> if some other manner of operating system
    */
    public static final boolean isWindows () {
        return OSPlatformUtil.isWindows();
    }

   /** Test whether the IDE is running on some variant of Unix.
    * Linux is included as well as the commercial vendors.
    * @return <code>true</code> some sort of Unix, <code>false</code> if some other manner of operating system
    */
    public static final boolean isUnix () {
        return OSPlatformUtil.isUnix();
    }

    public static boolean changeReadOnly( File dir, String filespec, boolean makeRO ) {

//        System.out.println("Operating system is "+System.getProperty(OS_PROPERTY_NAME));
        if (isWindows()) {
            if (operatingSystem == OSPlatformUtil.OS_WIN95 || operatingSystem == OSPlatformUtil.OS_WIN98) {
                boolean success = changeReadOnlyWin95(dir,filespec,makeRO);
                if (!success) {
                    success = changeReadOnlyWin95_V2(dir,filespec,makeRO);
                }
                return success;
            } else if (operatingSystem == OSPlatformUtil.OS_WIN2000) {
                return changeReadOnlyWin2000(dir,filespec,makeRO);
            } else if (operatingSystem == OSPlatformUtil.OS_WINXP) {
                return changeReadOnlyWin2000(dir,filespec,makeRO);
            } else {
                boolean success = changeReadOnlyWinNT(dir,filespec,makeRO);
                if (!success) {
                    success = changeReadOnlyWinNT_V2(dir,filespec,makeRO);
                }
                return success;
            }
        } if (isUnix()) {
            return changeReadOnlyUnix(dir,filespec,makeRO);
        }
        throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0056,  System.getProperty(OSPlatformUtil.OS_PROPERTY_NAME)));
//        if (osName.equalsIgnoreCase( "Windows NT")) {
//            return changeReadOnlyWinNT(dir,filespec,makeRO);
//        } else if (osName.equalsIgnoreCase("Windows 95")) {
//            return changeReadOnlyWin95(dir,filespec,makeRO);
//        } else if (osName.equalsIgnoreCase("Solaris")) {
//            return changeReadOnlySolaris(dir,filespec,makeRO);
//        } else {
//            throw new AssertionException("A native implementation of this method is not available for the \"" + osName + "\" operating system");
//        }
    }

    private static boolean changeReadOnlyUnix( File dir, String filespec, boolean makeRO ) {
        Runtime rt;
        Process proc;
        int exitcode;
        String dirpath;
        String[] attrib = new String[2];

        attrib[0] = "chmod"; //$NON-NLS-1$

        if ( makeRO ) {
            attrib[1] = "u-w"; //$NON-NLS-1$
        } else {
            attrib[1] = "u+wr,g+wr"; //$NON-NLS-1$
        }
        try {
            dirpath = dir.getCanonicalPath();
            attrib[1] = attrib[1] + " " + dirpath + SEPARATOR + filespec + ""; //$NON-NLS-1$ //$NON-NLS-2$
            rt = Runtime.getRuntime();

            String cmd = attrib[0] + " " + attrib[1]; //$NON-NLS-1$
            //printRuntimeCmd("changeReadOnlySolaris",cmd);
            //proc = rt.exec( attrib );
            proc = rt.exec( cmd );

            proc.waitFor();
            exitcode = proc.exitValue();
        } catch( Exception e ) {
            return( false );
        }
        return( exitcode == 0 );
    }

    private static boolean changeReadOnlyWin95( File dir, String filespec, boolean makeRO ) {
        Runtime rt;
        Process proc;
        int exitcode;
        String dirpath;
        String[] attrib = new String[2];

        attrib[0] = "c:\\windows\\command\\attrib.exe"; //$NON-NLS-1$
        //attrib[0] = "attrib -h -s";

        if ( makeRO ) {
            attrib[1] = "+R"; //$NON-NLS-1$
        } else {
            attrib[1] = "-R"; //$NON-NLS-1$
        }
        try {
            dirpath = dir.getCanonicalPath();
            attrib[1] = attrib[1] + " \"" + dirpath + SEPARATOR + filespec + "\""; //$NON-NLS-1$ //$NON-NLS-2$
            rt = Runtime.getRuntime();
            String cmd = attrib[0] + " " + attrib[1]; //$NON-NLS-1$
            //printRuntimeCmd("changeReadOnlyWin95",cmd);
            //proc = rt.exec( attrib );
            proc = rt.exec( cmd );

            proc.waitFor();
            exitcode = proc.exitValue();
        } catch( Exception e ) {
            return( false );
        }
        return( exitcode == 0 );
    }

    private static boolean changeReadOnlyWin95_V2( File dir, String filespec, boolean makeRO ) {
        Runtime rt;
        Process proc;
        int exitcode;
        String dirpath;
        String[] attrib = new String[2];

        attrib[0] = "attrib.exe"; //$NON-NLS-1$

        if ( makeRO ) {
            attrib[1] = "+R"; //$NON-NLS-1$
        } else {
            attrib[1] = "-R"; //$NON-NLS-1$
        }
        try {
            dirpath = dir.getCanonicalPath();
            attrib[1] = attrib[1] + " \"" + dirpath + SEPARATOR + filespec + "\""; //$NON-NLS-1$ //$NON-NLS-2$
            rt = Runtime.getRuntime();
            String cmd = attrib[0] + " " + attrib[1]; //$NON-NLS-1$
            proc = rt.exec( cmd );

            proc.waitFor();
            exitcode = proc.exitValue();
        } catch( Exception e ) {
            return( false );
        }
        return( exitcode == 0 );
    }

    private static boolean changeReadOnlyWin2000( File dir, String filespec, boolean makeRO ) {
        Runtime rt;
        Process proc;
        int exitcode;
        String dirpath;
        String[] attrib = new String[2];

        attrib[0] = "cmd.exe /c attrib"; //$NON-NLS-1$

        if ( makeRO ) {
            attrib[1] = "+R"; //$NON-NLS-1$
        } else {
            attrib[1] = "-R"; //$NON-NLS-1$
        }
        try {
            dirpath = dir.getCanonicalPath();
            attrib[1] = attrib[1] + " \"" + dirpath + SEPARATOR + filespec + "\""; //$NON-NLS-1$ //$NON-NLS-2$
            rt = Runtime.getRuntime();
            String cmd = attrib[0] + " " + attrib[1]; //$NON-NLS-1$
            //printRuntimeCmd("changeReadOnlyWin2000",cmd);
            //proc = rt.exec( attrib );
            proc = rt.exec( cmd );

            proc.waitFor();
            exitcode = proc.exitValue();
        } catch( Exception e ) {
            return( false );
        }
        return( exitcode == 0 );
    }

    private static boolean changeReadOnlyWinNT( File dir, String filespec, boolean makeRO ) {
        Runtime rt;
        Process proc;
        int exitcode;
        String dirpath;
        String[] attrib = new String[2];
        attrib[0] = "c:\\WINNT\\system32\\attrib.exe"; //$NON-NLS-1$
        //attrib[0] = "%SystemRoot%\\system32\\cmd.exe /c attrib";

        if ( makeRO ) {
            attrib[1] = "+R"; //$NON-NLS-1$
        } else {
            attrib[1] = "-R"; //$NON-NLS-1$
        }
        try {
            dirpath = dir.getCanonicalPath();
            attrib[1] = attrib[1] + " \"" + dirpath + SEPARATOR + filespec + "\""; //$NON-NLS-1$ //$NON-NLS-2$
            rt = Runtime.getRuntime();

            String cmd = attrib[0] + " " + attrib[1]; //$NON-NLS-1$
            //printRuntimeCmd("changeReadOnlyWinNT",cmd);
            //proc = rt.exec( attrib );
            proc = rt.exec( cmd );

            proc.waitFor();
            exitcode = proc.exitValue();
        } catch( Exception e ) {
            return( false );
        }
        return( exitcode == 0 );
    }

    private static boolean changeReadOnlyWinNT_V2( File dir, String filespec, boolean makeRO ) {
        Runtime rt;
        Process proc;
        int exitcode;
        String dirpath;
        String[] attrib = new String[2];
        attrib[0] = "attrib.exe"; //$NON-NLS-1$
        //attrib[0] = "%SystemRoot%\\system32\\cmd.exe /c attrib";

        if ( makeRO ) {
            attrib[1] = "+R"; //$NON-NLS-1$
        } else {
            attrib[1] = "-R"; //$NON-NLS-1$
        }
        try {
            dirpath = dir.getCanonicalPath();
            attrib[1] = attrib[1] + " \"" + dirpath + SEPARATOR + filespec + "\""; //$NON-NLS-1$ //$NON-NLS-2$
            rt = Runtime.getRuntime();

            String cmd = attrib[0] + " " + attrib[1]; //$NON-NLS-1$
            proc = rt.exec( cmd );

            proc.waitFor();
            exitcode = proc.exitValue();
        } catch( Exception e ) {
            return( false );
        }
        return( exitcode == 0 );
    }
    //*
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: PlatformAwareFilePermission dirPath fileName +r|-r"); //$NON-NLS-1$
            return;
        }
        try {
            // print system properties
            printSystemProperties(System.out);
            String dirPath    = args[0];
            String filename   = args[1];
            String permission = args[2];
            testChangePermission(dirPath, filename, permission);

//            String dirPath    = "E:/Products/current/projects/metadata/testdata";
//            String filename   = "ValidationTestModel4.xml";
//            testChangePermission(dirPath, filename, "-r");
//            testChangePermission(dirPath, filename, "+r");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    static void printSystemProperties( PrintStream stream ) {
        Assertion.isNotNull(stream,"The PrintStream reference may not be null"); //$NON-NLS-1$
        Properties p = System.getProperties();
        p.list(stream);
    }
    static void printPermissions(File f, String action) {
      System.out.println("\nFile \"" + f + "\" action = " + action); //$NON-NLS-1$ //$NON-NLS-2$
      System.out.println("exists  = "+f.exists()+" canRead = "+f.canRead()+" canWrite = "+f.canWrite()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    static void printRuntimeCmd(String methodName, String cmd) {
      System.out.println("\n[" + methodName + "] Runtime.exec( " + cmd + " )"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    static void testChangePermission(String dirPath, String filename, String permission) {
        boolean makeRO = true;
        if (permission.equalsIgnoreCase("-r")) { //$NON-NLS-1$
            makeRO = false;
        }
        File dir = new File(dirPath);
        File f   = new File(dir,filename);
        //PlatformAwareFilePermission.printPermissions(f,"before changing permission to "+permission);
        PlatformAwareFilePermission.changeReadOnly(dir,filename,makeRO);
        PlatformAwareFilePermission.printPermissions(f,"after changing permission to  "+permission); //$NON-NLS-1$
    }
    //*/
}


