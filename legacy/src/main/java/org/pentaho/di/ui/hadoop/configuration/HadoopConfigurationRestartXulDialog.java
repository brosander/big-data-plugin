package org.pentaho.di.ui.hadoop.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.hadoop.HadoopConfigurationInfo;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ShowHelpDialog;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.di.ui.spoon.XulSpoonSettingsManager;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.jface.tags.JfaceMenuList;
import org.pentaho.ui.xul.swt.SwtXulRunner;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 8/13/15.
 */
public class HadoopConfigurationRestartXulDialog extends AbstractXulEventHandler {
  private static final Class<?> PKG = HadoopConfigurationsXulDialog.class;
  private static final String CONTROLLER_NAME = "hadoopConfigurationRestartXulDialog";
  private static final String XUL = "org/pentaho/di/ui/hadoop/configuration/restart-prompt.xul";
  private static final Log logger = LogFactory.getLog( HadoopConfigurationRestartXulDialog.class );
  private final Shell shell;
  private XulDialog promptDialog;
  private boolean accept = false;

  public HadoopConfigurationRestartXulDialog( Shell aShell ) {
    this.shell = new Shell( aShell, SWT.SHELL_TRIM | SWT.APPLICATION_MODAL );
    setName( CONTROLLER_NAME );
  }

  public boolean open() {
    try {
      KettleXulLoader e = new KettleXulLoader();
      e.setOuterContext( shell );
      e.setSettingsManager( XulSpoonSettingsManager.getInstance() );
      e.registerClassLoader( getClass().getClassLoader() );
      XulDomContainer container = e.loadXul( XUL, new XulSpoonResourceBundle( PKG ) );
      container.addEventHandler( this );
      XulRunner runner = new SwtXulRunner();
      runner.addContainer( container );
      runner.initialize();
      promptDialog = (XulDialog) container.getDocumentRoot().getElementById( "hadoopConfigurationSelectionDialog" );
      promptDialog.show();
      ( (SwtDialog) promptDialog ).dispose();
    } catch ( Exception var4 ) {
      logger.info( var4 );
    }
    return accept;
  }

  public void cancel() {
    promptDialog.hide();
  }

  public void accept() {
    accept = true;
    promptDialog.hide();
  }

  public void showHelp() {
    ShowHelpDialog showHelpDialog =
      new ShowHelpDialog( shell,
        BaseMessages.getString( PKG, "HadoopConfigurationRestartXulDialog.Help.Title" ),
        BaseMessages.getString( PKG, "HadoopConfigurationRestartXulDialog.Help.Url" ),
        BaseMessages.getString( PKG, "HadoopConfigurationRestartXulDialog.Help.Header" ) ) {

        // Parent is modal so we have to be as well
        @Override protected Shell createShell( Shell parent ) {
          return new Shell( parent, SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL );
        }
      };
    showHelpDialog.open();
    showHelpDialog.dispose();
  }
}
