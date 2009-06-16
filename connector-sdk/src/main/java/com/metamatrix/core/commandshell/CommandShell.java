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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.MetaMatrixExceptionUtil;
import com.metamatrix.core.util.StringUtil;

/**
 * Reads input from standard input and directs commands read to the underlying target object.
 */
public class CommandShell implements Cloneable {
    private static final String RUN_METHOD = "run"; //$NON-NLS-1$
    private static final String COMMAND_PROMPT = ">"; //$NON-NLS-1$
    private static final String JAVA_LANG_PREFIX = "java.lang."; //$NON-NLS-1$

    
    public final static String DEFAULT_LOG_FILE = "../log/command_shell.log"; //$NON-NLS-1$
    
    private CommandTarget commandTarget;
    private boolean exceptionHandlingOn = false;
    private boolean printStackTraceOnException = true;
    private String defaultFilePath = ""; //$NON-NLS-1$
    private boolean quit = false;

    private Stack readerStack = new Stack();

    private boolean silent = true;
    
    private void setReader(ScriptReader reader) {
        readerStack.push(reader);
    }
    
    private ScriptReader getReader() {
        if (readerStack.isEmpty()) {
            return null;
        }
        return (ScriptReader) readerStack.peek();
    }
    
    private void clearReader() {
        readerStack.pop();
    }
    
    public CommandShell(CommandTarget commandTarget) {
        this.commandTarget = commandTarget;
        initialize();
    }
    
    public CommandShell copy(CommandTarget commandTarget) {
        try {
            CommandShell result = (CommandShell) clone();
            result.commandTarget = commandTarget;
            return result;
        } catch (CloneNotSupportedException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    private void initialize() {
        turnOnExceptionHandling();
        commandTarget.setShell(this);
    }
    public void setDefaultFilePath(String defaultFilePath) {
        this.defaultFilePath = defaultFilePath;
    }
    
    public String expandFileName(String localFileName) {
        return defaultFilePath + localFileName;
    }
    
    /**
     * Start reading commands from standard input.
     */
    public void run(String[] args, String logFile) {
        
        writeln(CorePlugin.Util.getString("CommandShell.Started")); //$NON-NLS-1$
        write(getCommandPrompt());
        if (args.length == 0) {
            enterInteractiveMode();
        } else {
            String[] commandArgs;
            boolean interactive = false;
            if (args[0].equals("-i")) { //$NON-NLS-1$
                interactive = true;
                commandArgs = new String[args.length-1];
                for (int i=1; i<args.length; i++) {
                    commandArgs[i-1] = args[i];
                }
            } else {
                commandArgs = args;
            }
            execute(commandArgs);
            if (interactive) {
                enterInteractiveMode();
            }
        }
        writeln(CorePlugin.Util.getString("CommandShell.Finished")); //$NON-NLS-1$
    }

      
    public void writeln(String text) {
        if (!silent) {
            System.out.println(text);
        }
    }
    
    public void write(String text) {
        if (!silent) {
            System.out.print(text);
        }
    }
    
    private void enterInteractiveMode() {        
        try {
            setSilent(false);
            execute("\n"); //$NON-NLS-1$ 
            
            InputStreamReader streamReader = new InputStreamReader(System.in);
            BufferedReader reader = new BufferedReader(streamReader);
            String line = reader.readLine();
            while (!quit && line !=null && !line.equals(".")) { //$NON-NLS-1$
                execute(line);
                if (quit) {
                } else {
                    line = reader.readLine();
                }
            }
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

	protected String getCommandPrompt() {
		return COMMAND_PROMPT;
	}

    public void quit() {
        quit = true;
    }
    
    public String[] getScriptNames(String fileName) {
        ScriptReader reader = new ScriptReader(new FileUtil(fileName).read());
        return reader.getScriptNames();
    }
    
    /**
     * Load a series of commands from a specific script in a file and execute the commands.
     * @param fileName The name of the file containing the script to run.
     * @param scriptName The name of the script within the file.
     * @param transcript Results of each command will be written to here.
     * @param listener When the script indicates the results should be checked, this object will be invoked.
     */
    public void runScript(String fileName, String scriptName, StringBuffer transcript, ScriptResultListener listener) {
        if (transcript == null) {
            transcript = new StringBuffer();
        }
        transcript.append(scriptName);
        transcript.append(" {"); //$NON-NLS-1$
        transcript.append(StringUtil.LINE_SEPARATOR);
        ScriptReader reader = new ScriptReader(new FileUtil(fileName).read());
        commandTarget.runningScript(fileName);
        setReader( reader );
        reader.gotoScript(scriptName);
        try {
            while (reader.hasMore()) {
                String commandLine = reader.nextCommandLine();
                writeln(CorePlugin.Util.getString("CommandShell.Executing", commandLine)); //$NON-NLS-1$
                transcript.append("\t"); //$NON-NLS-1$
                transcript.append(commandLine);
                String result = execute(commandLine);
                if (result == null) {
                    result = ""; //$NON-NLS-1$
                } else {
                    result = result.trim();
                }
                transcript.append(StringUtil.LINE_SEPARATOR);
                if (reader.checkResults()) {
                    transcript.append("\t"); //$NON-NLS-1$
                    transcript.append(ScriptReader.RESULT_KEYWORD);
                    transcript.append(" ["); //$NON-NLS-1$
                    transcript.append(StringUtil.LINE_SEPARATOR);
                    transcript.append(result);
                    transcript.append(StringUtil.LINE_SEPARATOR);
                    transcript.append(StringUtil.LINE_SEPARATOR);
                    transcript.append("\t"); //$NON-NLS-1$
                    transcript.append("]"); //$NON-NLS-1$
                    transcript.append(StringUtil.LINE_SEPARATOR);
                    String expectedResults = reader.getExpectedResults().trim();
                    if (listener != null) {
                        listener.scriptResults(fileName, scriptName, expectedResults, result);
                    }
                }
            }
        } finally {
            transcript.append("}"); //$NON-NLS-1$
            transcript.append(StringUtil.LINE_SEPARATOR);
            transcript.append(StringUtil.LINE_SEPARATOR);
            reader = null;
            clearReader();
        }
    }
    
    public String getNextCommandLine() throws IOException {
        ScriptReader reader = getReader();
        if (reader == null) {
            return new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
        return reader.nextCommandLine();
    }

    public Object getTarget() {
        return commandTarget;
    }
    
    public String getHelp() {
        Method[] methods = commandTarget.getClass().getMethods();
        StringBuffer result = new StringBuffer();
        
        Arrays.sort(methods, new Comparator(){
                public int compare(Object o1, Object o2) {
                    String name1 = ((Method) o1).getName();
                    String name2 = ((Method) o2).getName();
                    return name1.compareTo(name2);
                }
        });
        
        Set methodsToIgnore = commandTarget.getMethodsToIgnore();
        for (int i=0; i<methods.length; i++) {
            if (methods[i].getDeclaringClass() == Object.class) {
            } else {
                if (showHelpFor(methods[i].getName(), methodsToIgnore)) {
                    result.append(methods[i].getName());
                    result.append(" "); //$NON-NLS-1$
                    Class[] parameterTypes = methods[i].getParameterTypes();
                    String[] parameterNames = getParameterNames(methods[i].getName());
                    for (int j=0; j<parameterTypes.length; j++) {
                        String parameterTypeName = parameterTypes[j].getName();
                        boolean isArray = false;
                        if (parameterTypes[j].isArray()) {
                            isArray = true;
                            parameterTypeName = parameterTypes[j].getComponentType().getName();                        
                        }
                        if (parameterTypeName.startsWith(JAVA_LANG_PREFIX)) {
                            parameterTypeName = parameterTypeName.substring(JAVA_LANG_PREFIX.length());
                        }
                    
                        if (j < parameterNames.length) {
                            result.append("<"); //$NON-NLS-1$
                            result.append(parameterNames[j]);
                            result.append(">"); //$NON-NLS-1$
                        } else {
                            result.append(parameterTypeName);
                            if (isArray) {
                                result.append("[]"); //$NON-NLS-1$
                            }
                        }
                        result.append(" "); //$NON-NLS-1$                        
                    }
                    result.append(StringUtil.LINE_SEPARATOR);
                }
            }
        }
        return result.toString();
    }
    
    protected boolean showHelpFor(String methodName, Set methodsToIgnore) {
        return !Command.shouldIgnoreMethod(methodName, methodsToIgnore);
    }
    
    protected String[] getParameterNames(String methodName) {
        String[] result = getParameterNamesDirect(methodName);
        if (result == null) {
            result = new String[] {};
        }
        return result;
    }

    private String[] getParameterNamesDirect(String methodName) {
        if (methodName.equals("delete")) { //$NON-NLS-1$
            return new String[] {"multilineSqlTerminatedWith;"}; //$NON-NLS-1$
        } else if (methodName.equals("exec")) { //$NON-NLS-1$
            return new String[] { "multilineSqlTerminatedWith;"}; //$NON-NLS-1$
        } else if (methodName.equals("execute")) { //$NON-NLS-1$
            return new String[] { "query" }; //$NON-NLS-1$
        } else if (methodName.equals("getColumns") ) { //$NON-NLS-1$
            return new String[] {"schemaPattern", "tableNamePattern", "columnNamePattern"};  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if (methodName.equals("getCrossReference")) { //$NON-NLS-1$
            return new String[] { "primarySchema:String", "primaryTable:String","foreignSchema:String", "foreignTable:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        } else if( methodName.equals("getExportedKeys")) { //$NON-NLS-1$
            return new String[] { "catalog:String", "schema:String", "table:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if( methodName.equals("getImportedKeys")) { //$NON-NLS-1$
            return new String[] { "catalog:String", "schema:String", "table:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if( methodName.equals("getIndexInfo")) { //$NON-NLS-1$
            return new String[] {"schema:String", "table:String", "unique:boolean", "approximate:boolean" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        } else if( methodName.equals("getModels")) { //$NON-NLS-1$
            return new String[] {"catalog:String", "schemaPattern:String", "modelPattern:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if( methodName.equals("getPrimaryKeys")) { //$NON-NLS-1$
            return new String[] { "schema:String",  "table:String" }; //$NON-NLS-1$ //$NON-NLS-2$
        } else if(methodName.equals("getProcedureColumns")) { //$NON-NLS-1$
            return new String[] { "schemaPattern:String", "procedureNamePattern:String", "columnNamePattern:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if(methodName.equals("getProcedures")) { //$NON-NLS-1$
            return new String[] { "schemaPattern:String","procedureNamePattern:String" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("getTables")) { //$NON-NLS-1$
            return new String[] {"schemaPattern:String", "tableNamePattern:String" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("getUDTs")) { //$NON-NLS-1$ 
            return new String[] {"schemaPattern:String", "typeNamePattern:String" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("insert")) { //$NON-NLS-1$
            return new String[] {"multilineSqlTerminatedWith;"}; //$NON-NLS-1$
        } else if(methodName.equals("loadTest")) { //$NON-NLS-1$
            return new String [] {"command:String", "threadCounts:int[]" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("loadTestWithSetup")) { //$NON-NLS-1$
            return new String[] { "setupCommand:String",  "command:String",  "threadCounts:int[]" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if(methodName.equals("prepareStatement")) { //$NON-NLS-1$
            return new String [] { "statementName:String", "types:String[]" }; //$NON-NLS-1$ //$NON-NLS-2$
        } else if(methodName.equals("printResults")) { //$NON-NLS-1$
            return new String[] { "resultString:StringBuffer",  "results:ResultSet" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("printResultsDirect")) { //$NON-NLS-1$
            return new String[] { "resultString:StringBuffer", "results:PrintableResults" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("run")) { //$NON-NLS-1$
            return new String[] { "scriptName:String" }; //$NON-NLS-1$ 
        } else if(methodName.equals("runRep")) { //$NON-NLS-1$
            return new String[] { "repCount:int",  "scriptName:String" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("runScript")) { //$NON-NLS-1$
            return new String[] { "fileName:String",  "scriptName:String" }; //$NON-NLS-1$ //$NON-NLS-2$
        } else if(methodName.equals("runStatement")) { //$NON-NLS-1$
            return new String[] { "statementName:String", "params:String[]" }; //$NON-NLS-1$ //$NON-NLS-2$ 
        } else if(methodName.equals("select")) { //$NON-NLS-1$
            return new String[] { "multilineSqlTerminatedWith;" }; //$NON-NLS-1$
        } else if(methodName.equals("setAutoCommit")) { //$NON-NLS-1$
            return new String[] { "value:boolean" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setAutoGetMetadata")) { //$NON-NLS-1$
            return new String[] { "automaticallyGetMetadata:boolean" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setClassLoaderDebug")) { //$NON-NLS-1$
            return new String[] { "value:boolean" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setClassLoadingDriver")) { //$NON-NLS-1$
            return new String[] { "protocol:String", "driverName:String", "classpath:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if(methodName.equals("setConnection")) { //$NON-NLS-1$
            return new String[] {"server:String", "port:String", "vdb:String", "version:String", "user:String", "password:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        } else if(methodName.equals("setConnectionProperties")) { //$NON-NLS-1$
            return new String[] { "connectionProperties:Properties" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setConnectionUrl")) { //$NON-NLS-1$
            return new String[] { "url:String" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setDriverClass")) { //$NON-NLS-1$
            return new String[] { "className:String" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setFailOnError")) { //$NON-NLS-1$
            return new String[] { "failOnError:boolean" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setIgnoreQueryPlan")) { //$NON-NLS-1$
            return new String[] { "ignoreQueryPlan:boolean" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setJdbcMode")) { //$NON-NLS-1$
            return new String[] { "jdbcMode:boolean" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setLocalConfig")) { //$NON-NLS-1$
            return new String[] { "vdbName:String",  "vdbVersion:String",  "localConfigFilePath:String" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if(methodName.equals("setPrintStackOnError")) { //$NON-NLS-1$
            return new String[] { "printStackOnError:boolean" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setQueryDisplayInterval")) { //$NON-NLS-1$
            return new String[] {"n:int" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setQueryTimeout")) { //$NON-NLS-1$
            return new String[] {"timeoutInSeconds:int" }; //$NON-NLS-1$ 
        } else if(methodName.equals("setScriptFile")) { //$NON-NLS-1$
            return new String[] {"scriptFileName:String" }; //$NON-NLS-1$
        } else if(methodName.equals("setSilent")) { //$NON-NLS-1$
            return new String[] {"silent:boolean"}; //$NON-NLS-1$
        } else if(methodName.equals("setUsePreparedStatement")) { //$NON-NLS-1$
            return new String[] {"usePreparedStatement:boolean"}; //$NON-NLS-1$
        } else if(methodName.equals("singleLoadTest")) { //$NON-NLS-1$
            return new String[] {"setupCommand:String", "command:String", "threadCount:int"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else if(methodName.equals("update")) { //$NON-NLS-1$
            return new String[] {"multilineSqlTerminatedWith;"}; //$NON-NLS-1$
        }
        return null;
    }

    public void turnOnExceptionHandling() {
        exceptionHandlingOn = true;
    }

    public void turnOffExceptionHandling() {
        exceptionHandlingOn = false;
    }

    public void setPrintStackTraceOnException(boolean printStackTraceOnException) {
        this.printStackTraceOnException = printStackTraceOnException;
    }

    private String execute(Command command, boolean throwWrongNumberOfArgumentsException, String defaultResult ) throws NoSuchMethodException {
        Throwable exception = null;
        command.setDefaultFilePath(defaultFilePath);
        Object result = defaultResult;
        try {
            result = command.execute(commandTarget.getMethodsToIgnore());
        } catch (WrongNumberOfArgumentsException e) {
            if (throwWrongNumberOfArgumentsException) {
            } else {
                result = e.getMessage();
            }
        } catch (NoScriptFileException e) {
            if (throwWrongNumberOfArgumentsException) {
            } else {
                result = e.getMessage();
            }
        } catch (ArgumentConversionException e) {
            result = e.getMessage();
        } catch (NoSuchMethodException e) {
            throw e;
        } catch (Throwable e) {            
            if (exceptionHandlingOn) {
                result = MetaMatrixExceptionUtil.getLinkedMessagesVerbose(e);
                exception = e;
            } else {
                if (e instanceof RuntimeException) {
                    RuntimeException runtimeException = (RuntimeException) e;
                    throw runtimeException;
                }
                throw new MetaMatrixRuntimeException(e);
            }
        }
        
        String stringResult = null;
        if (result != null) {
			if( result instanceof String[] ) {
				String[] arrayResult = (String[]) result;
				StringBuffer sb = new StringBuffer();
				sb.append("["); //$NON-NLS-1$
				for (int i = 0; i < arrayResult.length; i++) {
					sb.append(arrayResult[i]);
					if( i != arrayResult.length-1) sb.append(","); //$NON-NLS-1$
				}
				sb.append("]"); //$NON-NLS-1$
				stringResult = sb.toString();
			} else {
				stringResult = result.toString();
			}
            writeln(stringResult);
        }
        if (printStackTraceOnException) {
            if (exception != null) {
                MetaMatrixExceptionUtil.printNestedStackTrace(exception, System.out);
                //exception.printStackTrace(System.out);
            }
        }
        write(getCommandPrompt()); 
        return stringResult;
    }

    /**
     * Run a single command as if it was entered on standard input.
     * @param commandLine The command to execute.
     * @return The results of executing the command on the target object.
     */
    public String execute(String commandLine) {
        try {
            return executeCommand(new Command(getTarget(), commandLine), new Command(getTarget(), RUN_METHOD + " " + commandLine)); //$NON-NLS-1$
        } catch (NoSuchMethodException e) {
            return e.getMessage();
        }
    }

    private String executeCommand(Command command, Command alternateCommand) throws NoSuchMethodException {
        try {
            return execute(command, false, null);
        } catch (NoSuchMethodException e) {
            try {
                //try to run the command as a script
                return execute(alternateCommand, true, e.getMessage());
            } catch (NoSuchMethodException e2) {
                throw e;
            }
        }
    }

    private String execute(String[] commandArgs) {
        Command c = new Command(getTarget(), commandArgs);
        Command c2 = new Command(getTarget(), makeStringArray(RUN_METHOD, commandArgs));
        try {
            return executeCommand(c, c2);
        } catch (NoSuchMethodException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    private String[] makeStringArray(String firstValue, String[] otherValues) {
        List result = new ArrayList(Arrays.asList(otherValues));
        result.add(0, firstValue);
        return (String[]) result.toArray(new String[] {});
    }

    public void setSilent(boolean silent) {
        this.silent = silent;
    }  
}
