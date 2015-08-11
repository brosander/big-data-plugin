package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.ui.core.namedcluster.HadoopClusterDelegate;
import org.pentaho.metastore.api.IMetaStore;

/**
 * Created by bryan on 8/17/15.
 */
public class HadoopClusterDelegateBridgeImpl implements HadoopClusterDelegate {
  private final HadoopClusterDelegateImpl delegate;

  public HadoopClusterDelegateBridgeImpl( HadoopClusterDelegateImpl delegate ) {
    this.delegate = delegate;
  }

  @Override public String editNamedCluster( IMetaStore metaStore, NamedCluster namedCluster, Shell shell ) {
    return delegate.editNamedCluster( metaStore, new NamedClusterBridgeImpl( namedCluster ), shell );
  }

  @Override public String newNamedCluster( VariableSpace variableSpace, IMetaStore metaStore, Shell shell ) {
    return delegate.newNamedCluster( variableSpace, metaStore, shell );
  }

  @Override public void dupeNamedCluster( IMetaStore metaStore, NamedCluster nc, Shell shell ) {
    delegate.dupeNamedCluster( metaStore, new NamedClusterBridgeImpl( nc ), shell );
  }

  @Override public void delNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    delegate.delNamedCluster( metaStore, new NamedClusterBridgeImpl( namedCluster ) );
  }
}
