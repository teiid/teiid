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

package com.metamatrix.core.commandshell;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.InvalidIDException;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.StringUtil;

/**
 * Executes a command string on a given object.  The command string is expected to be of the form:
 * <methodName> <arg1> <arg2> ...
 * Where the method name is the name of a method on the target object and where the argument count matches the signature
 * of the method on the target object and where the argument strings can be converted to the types of the arguments in 
 * the target method.
 */
public class Command {
    private static final String COMMAND_COMMENT = "//"; //$NON-NLS-1$
    
    private Object target;
    private Class targetClass;
    private String commandName;
    private String[] args;
    private Method method;
    
    private String defaultFilePath = ""; //$NON-NLS-1$
    
    /**
     * Instantiate a command for a target object.
     * @param target  The object to execute the command on.
     * @param commandLine The substrings making up a single command.
     */
    public Command(Object target, String[] commandLine) {
        init(target, commandLine);
    }
    
    private void init(Object target, String[] commandLine) {
        this.target = target;
        this.targetClass = target.getClass();
        
        if (commandLine.length == 0) {
            commandName = null;
        } else {
            commandName = commandLine[0];
            if (commandName.trim().startsWith(COMMAND_COMMENT)) {
                commandName = null;
            } else {
                args = new String[commandLine.length-1];
                for (int i=1; i<commandLine.length; i++) {
                    args[i-1] = commandLine[i];
                }
            }
        }
    }
    
    /**
     * Instantiate a command for a target object.
     * @param target The object to execute the command on.
     * @param commandLine The command to execute.
     */
    public Command(Object target, String commandLine) {
        init(target, new CommandLineParser().parse(commandLine));
    }
    
    /**
     * Insantiate a command for a target object.
     * @param target The object to execute the command on.
     * @param commandName The name of the method to execute.
     * @param args The arguments for the method to execute.
     */
    public Command(Object target, String commandName, String[] args) {
        this.target = target;
        this.targetClass = target.getClass();
        this.commandName = commandName;
        this.args = args;
    }

    public void setDefaultFilePath(String defaultFilePath) {
        this.defaultFilePath = defaultFilePath;
    }
    
    private Method getMethod(Set methodsToIgnore) throws NoSuchMethodException {
        if (shouldIgnoreMethod(commandName, methodsToIgnore)) {
        } else {
            getMethodDirect();
        }
        if (method == null) {
            Object[] params = new Object[] {targetClass.getName(), commandName};
            String message = CorePlugin.Util.getString("Command.Could_not_find_method", params); //$NON-NLS-1$            
            throw new NoSuchMethodException(message);
        }
        return method;
    }

    static boolean shouldIgnoreMethod(String currentMethodName, Set methodsToIgnore) {
        for (Iterator i = methodsToIgnore.iterator(); i.hasNext(); ) {
            String methodName = (String) i.next();
            if (methodName.toLowerCase().equals(currentMethodName.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    private void getMethodDirect() {
        Method[] methods = targetClass.getMethods();
        for (int i=0; i<methods.length; i++) {
            if (shouldSkipMethod(methods, i)) {
            } else {
                if (methods[i].getName().toLowerCase().equals(commandName.toLowerCase())) {
                    method = methods[i];
                    break;
                }
            }
        }
    }

    private boolean shouldSkipMethod(Method[] methods, int i) {
        boolean skipMethod = false;
        if (methods[i].getDeclaringClass().equals(Object.class)) {
            skipMethod = true;
        }
        if (methods[i].getDeclaringClass().equals(CommandTarget.class)) {
            if (commandName.toLowerCase().equals("help") || commandName.toLowerCase().equals("quit") || commandName.toLowerCase().equals("exit")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$                         
            } else {
                skipMethod = true;
            }
        }
        return skipMethod;
    }
    
    private Object[] getConvertedArgs() throws WrongNumberOfArgumentsException, ArgumentConversionException {
        Class[] neededTypes = method.getParameterTypes();
        if (neededTypes.length == 1) {
            String[] stringArray = new String[] {};
            if (neededTypes[0] == stringArray.getClass()) {
                return new Object[] {args};                
            }
        }
        
        boolean skipLast = false;
        Object lastArg = null;
        if (neededTypes.length > 0) {
            Class lastArgType = neededTypes[neededTypes.length-1];
            if (lastArgType.isArray()) {
                Object[] tempLastArg = new Object[args.length - (neededTypes.length-1)];
                for (int i=neededTypes.length-1; i<args.length; i++) {
                    tempLastArg[i-(neededTypes.length-1)] = convert(args[i], lastArgType.getComponentType());
                }
                skipLast = true;
                lastArg = convertArray(tempLastArg, lastArgType.getComponentType());
            }
        }

        if (skipLast) {
            if (neededTypes.length > args.length) {
                Object[] params = new Object[] {new Integer(neededTypes.length), new Integer(args.length)};
                String message = CorePlugin.Util.getString("Command.Argument_count_mis-match,_expected_{0}_but_received_{1}", params); //$NON-NLS-1$
                throw new WrongNumberOfArgumentsException(message);
            }
        } else {
            if (neededTypes.length != args.length) {
                Object[] params = new Object[] {new Integer(neededTypes.length), new Integer(args.length)};
                String message = CorePlugin.Util.getString("Command.Argument_count_mis-match,_expected_{0}_but_received_{1}", params); //$NON-NLS-1$
                throw new WrongNumberOfArgumentsException(message);
            }
        }
        Object[] result = new Object[neededTypes.length];
        if (skipLast) {
            for (int i=0; i<neededTypes.length-1; i++) {
                result[i] = convert(args[i], neededTypes[i]);
            }
            result[neededTypes.length-1] = lastArg;
        } else {
            for (int i=0; i<neededTypes.length; i++) {
                result[i] = convert(args[i], neededTypes[i]);
            }
        }
        return result;
    }
    
    private Object convertArray(Object[] input, Class targetType) {
        Object result = Array.newInstance(targetType, input.length);
        for (int i=0; i<input.length; i++) {
            Array.set(result, i, input[i]);
        }
        return result;
    }
    
    private void handleFileException(String target, Throwable exception) throws ArgumentConversionException {
        String message = CorePlugin.Util.getString("Command.Error_processing_file", target);         //$NON-NLS-1$
        throw new ArgumentConversionException(exception, message);
    }
    
    private Object convert(String target, Class neededType) throws ArgumentConversionException {
        if (neededType.equals(String.class)) {
            return target;
        }
        byte[] x = new byte[0];
        if (neededType.equals(x.getClass())) {
            String fileName = defaultFilePath + target;
            FileUtil file = new FileUtil(fileName);
            try {
                return file.readBytesSafe();
            } catch (FileNotFoundException e) {
                handleFileException(target, e);
            } catch (IOException e) {
                handleFileException(target, e);
            }
        }
        if (neededType.equals(Date.class)) {
            try {
                return DateUtil.convertStringToDate(target);
            } catch (ParseException e) {
                throw new ArgumentConversionException(e, e.getMessage());
            }
        }
        if (neededType.equals(Integer.TYPE)) {
            return new Integer(target);
        }
        if (neededType.equals(Boolean.TYPE)) {
            return new Boolean(target);
        }
        if (neededType.equals(Properties.class)) {
            Properties result = new Properties();
            String[] subStrings = (String[]) StringUtil.split(target, ",=").toArray(new String[]{}); //$NON-NLS-1$
            for (int i=0; i<subStrings.length; i=i+2) {
                result.put(subStrings[i], subStrings[i+1]);
            }
            return result;
        }
        if (neededType.equals(ObjectID.class)) {
            if (target != null) {
                try {
                    if (target.equals("null")) { //$NON-NLS-1$
                        return null;
                    }
                    return IDGenerator.getInstance().stringToObject(target);
                } catch (InvalidIDException e) {
                    throw new ArgumentConversionException(e, e.getMessage());
                }
            }
            return null;
        }
        char[] charArray = new char[0];
        if( neededType.equals(charArray.getClass())) {
            String fileName = defaultFilePath + target;
            FileUtil file = new FileUtil(fileName);
            try {
                return file.readSafe().toCharArray();
            } catch (FileNotFoundException e) {
                handleFileException(target, e);
            } 
        }
        
        String message = CorePlugin.Util.getString("Command.Cannot_convert_to", neededType); //$NON-NLS-1$
        throw new MetaMatrixRuntimeException( message );
    }
    
    public Object execute() throws NoSuchMethodException, WrongNumberOfArgumentsException, ArgumentConversionException {
        return execute(Collections.EMPTY_SET);
    }
    
    /**
     * Execute the command on the target object.
     * @param methodsToIgnore contains the names of methods to exclude from the methods searched for matches.
     * @return The results of reflectively invoking the command on the target object.
     * @throws NoSuchMethodException
     * @throws WrongNumberOfArgumentsException
     * @throws ArgumentConversionException
     */
    public Object execute(Set methodsToIgnore) throws NoSuchMethodException, WrongNumberOfArgumentsException, ArgumentConversionException {
        if (commandName == null) {
            return null;
        }
        getMethod(methodsToIgnore);
        try {
            return method.invoke(target, getConvertedArgs());
        } catch (IllegalArgumentException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof MetaMatrixRuntimeException) {
                throw (MetaMatrixRuntimeException) e.getTargetException();
            } if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new MetaMatrixRuntimeException(e);
        }
    }
}
