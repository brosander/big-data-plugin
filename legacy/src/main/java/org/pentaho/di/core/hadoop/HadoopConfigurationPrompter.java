package org.pentaho.di.core.hadoop;

import java.util.List;

/**
 * Created by bryan on 8/13/15.
 */
public interface HadoopConfigurationPrompter {
  String getConfigurationSelection( List<HadoopConfigurationInfo> hadoopConfigurationInfos );

  boolean promptForRestart();
}
