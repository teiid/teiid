/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.adminapi.jboss;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

import org.jboss.as.cli.*;
import org.jboss.as.cli.batch.BatchManager;
import org.jboss.as.cli.batch.BatchedCommand;
import org.jboss.as.cli.operation.CommandLineParser;
import org.jboss.as.cli.operation.NodePathFormatter;
import org.jboss.as.cli.operation.OperationCandidatesProvider;
import org.jboss.as.cli.operation.OperationRequestAddress;
import org.jboss.as.cli.operation.ParsedCommandLine;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.controller.client.Operation;
import org.jboss.dmr.ModelNode;

public class DMRCommandContext implements CommandContext {
    private CommandContext delegate;
    private ModelControllerClient mcc;

    public CliConfig getConfig() {
        return delegate.getConfig();
    }

    public String getArgumentsString() {
        return delegate.getArgumentsString();
    }

    public ParsedCommandLine getParsedCommandLine() {
        return delegate.getParsedCommandLine();
    }

    public void printLine(String message) {
        delegate.printLine(message);
    }

    public void printColumns(Collection<String> col) {
        delegate.printColumns(col);
    }

    public void clearScreen() {
        delegate.clearScreen();
    }

    public void terminateSession() {
        delegate.terminateSession();
    }

    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    public void set(Scope scope, String key, Object value) {
        delegate.set(scope, key, value);
    }

    public Object get(Scope scope, String key) {
        return delegate.get(scope, key);
    }

    public void clear(Scope scope) {
        delegate.clear(scope);
    }

    public Object remove(Scope scope, String key) {
        return delegate.remove(scope, key);
    }

    public ModelControllerClient getModelControllerClient() {
        return mcc;
    }

    public void setModelControllerClient(ModelControllerClient mcc) {
        this.mcc = mcc;
    }

    public void connectController() throws CommandLineException {
        delegate.connectController();
    }

    public void connectController(String controller) throws CommandLineException {
        delegate.connectController(controller);
    }

    public void connectController(String host, int port) throws CommandLineException {
        delegate.connectController(host, port);
    }

    public void bindClient(ModelControllerClient newClient) {
        delegate.bindClient(newClient);
    }

    public void disconnectController() {
        delegate.disconnectController();
    }

    public String getDefaultControllerHost() {
        return delegate.getDefaultControllerHost();
    }

    public int getDefaultControllerPort() {
        return delegate.getDefaultControllerPort();
    }

    public ControllerAddress getDefaultControllerAddress() {
        return delegate.getDefaultControllerAddress();
    }

    public String getControllerHost() {
        return delegate.getControllerHost();
    }

    public int getControllerPort() {
        return delegate.getControllerPort();
    }

    public CommandLineParser getCommandLineParser() {
        return delegate.getCommandLineParser();
    }

    public OperationRequestAddress getCurrentNodePath() {
        return delegate.getCurrentNodePath();
    }

    public NodePathFormatter getNodePathFormatter() {
        return delegate.getNodePathFormatter();
    }

    public OperationCandidatesProvider getOperationCandidatesProvider() {
        return delegate.getOperationCandidatesProvider();
    }

    public CommandHistory getHistory() {
        return delegate.getHistory();
    }

    public boolean isBatchMode() {
        return delegate.isBatchMode();
    }

    public boolean isWorkflowMode() {
        return delegate.isWorkflowMode();
    }

    public BatchManager getBatchManager() {
        return delegate.getBatchManager();
    }

    public BatchedCommand toBatchedCommand(String line) throws CommandFormatException {
        return delegate.toBatchedCommand(line);
    }

    public ModelNode buildRequest(String line) throws CommandFormatException {
        return delegate.buildRequest(line);
    }

    public CommandLineCompleter getDefaultCommandCompleter() {
        return delegate.getDefaultCommandCompleter();
    }

    public boolean isDomainMode() {
        return delegate.isDomainMode();
    }

    public void addEventListener(CliEventListener listener) {
        delegate.addEventListener(listener);
    }

    public int getExitCode() {
        return delegate.getExitCode();
    }

    public void handle(String line) throws CommandLineException {
        delegate.handle(line);
    }

    public void handleSafe(String line) {
        delegate.handleSafe(line);
    }

    public void interact() {
        delegate.interact();
    }

    public File getCurrentDir() {
        return delegate.getCurrentDir();
    }

    public void setCurrentDir(File dir) {
        delegate.setCurrentDir(dir);
    }

    public boolean isResolveParameterValues() {
        return delegate.isResolveParameterValues();
    }

    public void setResolveParameterValues(boolean resolve) {
        delegate.setResolveParameterValues(resolve);
    }

    public boolean isSilent() {
        return delegate.isSilent();
    }

    public void setSilent(boolean silent) {
        delegate.setSilent(silent);
    }

    public int getTerminalWidth() {
        return delegate.getTerminalWidth();
    }

    public int getTerminalHeight() {
        return delegate.getTerminalHeight();
    }

    public void setVariable(String name, String value) throws CommandLineException {
        delegate.setVariable(name, value);
    }

    public String getVariable(String name) {
        return delegate.getVariable(name);
    }

    public Collection<String> getVariables() {
        return delegate.getVariables();
    }

    public void registerRedirection(CommandLineRedirection redirection) throws CommandLineException {
        delegate.registerRedirection(redirection);
    }

    public ConnectionInfo getConnectionInfo() {
        return delegate.getConnectionInfo();
    }

    public void captureOutput(PrintStream captor) {
        delegate.captureOutput(captor);
    }

    public void releaseOutput() {
        delegate.releaseOutput();
    }

    public void setCommandTimeout(int numSeconds) {
        delegate.setCommandTimeout(numSeconds);
    }

    public int getCommandTimeout() {
        return delegate.getCommandTimeout();
    }

    public void resetTimeout(TIMEOUT_RESET_VALUE value) {
        delegate.resetTimeout(value);
    }

    public ModelNode execute(ModelNode mn, String description) throws CommandLineException, IOException {
        return delegate.execute(mn, description);
    }

    public ModelNode execute(Operation op, String description) throws CommandLineException, IOException {
        return delegate.execute(op, description);
    }

    public DMRCommandContext(CommandContext delegate) {
        this.delegate = delegate;
    }

    @Override
    public void connectController(String arg0, String arg1)
            throws CommandLineException {
        this.delegate.connectController(arg0, arg1);
    }
}
