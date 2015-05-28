package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;

/**
 * Created by bryan on 5/27/15.
 */
public class ShimHadoopFileSystemLocator {
    public void blah() throws ConfigurationException {
        HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration().getHadoopShim();
        KettleVFS.getInstance().getFileSystemManager().
    }
}
