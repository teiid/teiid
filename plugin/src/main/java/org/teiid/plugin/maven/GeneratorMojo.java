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
package org.teiid.plugin.maven;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.model.Dependency;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.repository.ArtifactDoesNotExistException;
import org.apache.maven.repository.ArtifactTransferEvent;
import org.apache.maven.repository.ArtifactTransferFailedException;
import org.apache.maven.repository.ArtifactTransferListener;

@Mojo(
        name = "modules-generator",
        defaultPhase = LifecyclePhase.GENERATE_RESOURCES,
        requiresDependencyCollection = ResolutionScope.COMPILE_PLUS_RUNTIME,
        requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME
)
public class GeneratorMojo extends AbstractTeiidMojo {
    
    @Parameter(alias = "codeDir")
    protected String teiidCodebaseDir;
    
    @Parameter(alias = "targetDir")
    protected String modulesTargetDir;
    
    @Parameter(alias = "featurePackPomPath")
    protected String featurePackPomPath;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        
        Set<String> modules = new HashSet<>();
        if(this.teiidCodebaseDir != null){
            try(BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("teiid-codebase-modules.txt")))){ //$NON-NLS-1$
                for(String line; (line = br.readLine()) != null; ){
                    Path modulePath = Paths.get(teiidCodebaseDir, line.trim());
                    if(Files.exists(modulePath) && Files.isDirectory(modulePath)){
                        modules.add(modulePath.toString());
                    } else {
                        getLog().warn(modulePath + " not exist"); //$NON-NLS-1$
                    }
                }
            } catch(IOException e) {
                throw new MojoExecutionException(e.getMessage());
            }
        } else {
            File zipFile = getWildflyDist();
            Path path = Paths.get(getProjectBuildDir(), "modules-generator"); //$NON-NLS-1$
            try {
                unzipModules(zipFile, path.toString());
            } catch (IOException e) {
                throw new MojoExecutionException("unzip" + zipFile + " failed", e); //$NON-NLS-1$ //$NON-NLS-2$
            }
            Path unpackModule = Paths.get(path.toString(), "modules/system/layers/dv"); //$NON-NLS-1$
            modules.add(unpackModule.toString());
        }
        
        if(modules.size() == 0) {
            throw new MojoExecutionException("Can not find modules"); //$NON-NLS-1$
        }
        
        List<Dependency> dependencies = getProjectependencies();
        List<String> dependencyArtifact = new ArrayList<>(dependencies.size());
        for(Dependency d : dependencies) {
            dependencyArtifact.add(d.getGroupId() + ":" + d.getArtifactId());
        }
        
        if(this.modulesTargetDir == null){
            this.modulesTargetDir = Paths.get(getProjectBuildDir(), "classes").toString(); //$NON-NLS-1$
        }    
        Path modulesTargetPath = Paths.get(modulesTargetDir, "modules/system/layers/dv"); //$NON-NLS-1$
        Path featurePackPom = this.featurePackPomPath == null ? null : Paths.get(this.featurePackPomPath);
        
        try {
            if(!Files.exists(modulesTargetPath)){
                Files.createDirectories(modulesTargetPath);
            }
            new Generator(modulesTargetPath, featurePackPom, modules, dependencyArtifact, getLog()).processGeneratorTargets();
        } catch (IOException e) {
            throw new MojoExecutionException("Generate Modules Error", e); //$NON-NLS-1$
        }
    }

    private void unzipModules(File zipFile, String target) throws IOException {

        ZipFile zip = new ZipFile(zipFile);
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()){
            ZipEntry entry = entries.nextElement();
            String name = entry.getName();
            if(name.startsWith("modules/system/layers/dv") && (name.endsWith("module.xml") || name.endsWith("ra.xml") || name.endsWith("MANIFEST.MF"))){ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                Path targetPath = Paths.get(target, name);
                if(!Files.exists(targetPath)){
                    Files.createDirectories(targetPath.getParent());
                    Files.createFile(targetPath);
                }
                try(
                        InputStream in = zip.getInputStream(entry);
                        OutputStream fos = new FileOutputStream(targetPath.toFile());){
                    byte[] buf = new byte[1024];
                    int length;
                    while ((length = in.read(buf)) > 0) {
                        fos.write(buf, 0, length);
                    }
                }           
            }            
        }        
        zip.close();
    }

    private File getWildflyDist() {
        String artifactId = "teiid"; //$NON-NLS-1$
        String classifier = "wildfly-dist"; //$NON-NLS-1$
        String type = "zip"; //$NON-NLS-1$
        PluginDescriptor plugin = (PluginDescriptor) getPluginContext().get("pluginDescriptor"); //$NON-NLS-1$
        String groupId = plugin.getGroupId();
        String version = plugin.getVersion();
//        String version = "9.1.0.Alpha2";
        String line = "-";
        String artifact = artifactId + line + version + line + classifier + "." + type;
        
        ArtifactRepository jbossDeveloper = null;
        for(ArtifactRepository repository : remoteRepositories){
            if(repository.getUrl().startsWith("https://repository.jboss.org/nexus/content/groups/developer")){ //$NON-NLS-1$
                jbossDeveloper = repository;
                break;
            }     
        }
        
      groupId = groupId.replaceAll("\\.", File.separator) + File.separator + artifactId + File.separator; //$NON-NLS-1$
      String remotePath = groupId + version + File.separator + artifact;
      getLog().info("Download " + artifact + " from " + jbossDeveloper.getUrl()); //$NON-NLS-1$ //$NON-NLS-2$
      File destination = null;
      try {
          destination = File.createTempFile(artifactId, "." + type);
          destination.deleteOnExit();
          repoSystem.retrieve(jbossDeveloper, destination, remotePath, new ArtifactTransferListener(){

              @Override
              public boolean isShowChecksumEvents() {
                  return false;
              }

              @Override
              public void setShowChecksumEvents(boolean showChecksumEvents) {                            
              }

              @Override
              public void transferInitiated(ArtifactTransferEvent transferEvent) {                            
              }

              @Override
              public void transferStarted(ArtifactTransferEvent transferEvent) {  
                  getLog().info("download start"); //$NON-NLS-1$
              }

              @Override
              public void transferProgress(ArtifactTransferEvent transferEvent) {  
              }

              @Override
              public void transferCompleted(ArtifactTransferEvent transferEvent) {
                  getLog().info("download completed"); //$NON-NLS-1$
              }});
      } catch (IOException | ArtifactTransferFailedException | ArtifactDoesNotExistException e) {
      }
      
      return destination;
    }

}
