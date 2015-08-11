package org.pentaho.di.ui.core.namedcluster;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.ui.spoon.Spoon;

/**
 * Created by bryan on 8/17/15.
 */
public interface NamedClusterUIFactory {
  NamedClusterWidget createNamedClusterWidget( Composite parent, boolean showLabel );

  HadoopClusterDelegate createHadoopClusterDelegate( Spoon spoon );

  NamedClusterDialog createNamedClusterDialog( Shell shell );
}
