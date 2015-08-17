package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.pentaho.big.data.plugins.common.ui.NamedClusterDialogImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.core.namedcluster.NamedClusterDialog;

/**
 * Created by bryan on 8/17/15.
 */
public class NamedClusterDialogBridgeImpl implements NamedClusterDialog {
  private final NamedClusterDialogImpl delegate;

  public NamedClusterDialogBridgeImpl( NamedClusterDialogImpl delegate ) {
    this.delegate = delegate;
  }

  @Override public NamedCluster getNamedCluster() {
    return NamedClusterBridgeImpl.fromOsgiNamedCluster( delegate.getNamedCluster() );
  }

  @Override public void setNamedCluster( NamedCluster namedCluster ) {
    delegate.setNamedCluster( new NamedClusterBridgeImpl( namedCluster ) );
  }

  @Override public void setNewClusterCheck( boolean newClusterCheck ) {
    delegate.setNewClusterCheck( newClusterCheck );
  }

  @Override public String open() {
    return delegate.open();
  }
}
