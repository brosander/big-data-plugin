package org.pentaho.di.ui.core.namedcluster;

import org.pentaho.di.core.namedcluster.model.NamedCluster;

/**
 * Created by bryan on 8/17/15.
 */
public interface NamedClusterDialog {
  void setNamedCluster( NamedCluster namedCluster );

  NamedCluster getNamedCluster();

  void setNewClusterCheck( boolean newClusterCheck );

  String open();
}
