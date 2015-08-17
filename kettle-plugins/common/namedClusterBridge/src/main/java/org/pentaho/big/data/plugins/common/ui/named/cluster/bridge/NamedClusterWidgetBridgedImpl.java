package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.core.namedcluster.NamedClusterWidget;

/**
 * Created by bryan on 8/17/15.
 */
public class NamedClusterWidgetBridgedImpl implements NamedClusterWidget {
  private final NamedClusterWidgetImpl namedClusterWidget;

  public NamedClusterWidgetBridgedImpl( NamedClusterWidgetImpl namedClusterWidget ) {
    this.namedClusterWidget = namedClusterWidget;
  }

  @Override public void initiate() {
    namedClusterWidget.initiate();
  }

  @Override public Composite getComposite() {
    return namedClusterWidget;
  }

  @Override public NamedCluster getSelectedNamedCluster() {
    return NamedClusterBridgeImpl.fromOsgiNamedCluster( namedClusterWidget.getSelectedNamedCluster() );
  }

  @Override public void setSelectedNamedCluster( String name ) {
    namedClusterWidget.setSelectedNamedCluster( name );
  }

  @Override public void addSelectionListener( SelectionListener selectionListener ) {
    namedClusterWidget.addSelectionListener( selectionListener );
  }
}
