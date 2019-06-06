package com.weibo.breeze.maven.plugin;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * Created by zhanglei28 on 2019/5/16.
 */
@Mojo(name = "compile", defaultPhase = LifecyclePhase.COMPILE)
public class CodeMojo extends AbstractMojo {

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        //TODO compile from *.breeze
    }
}
