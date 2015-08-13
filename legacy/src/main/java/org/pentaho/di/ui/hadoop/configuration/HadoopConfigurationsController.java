/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.di.ui.hadoop.configuration;

import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopConfigurationInfo;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.jface.tags.JfaceMenupopup;

import java.io.IOException;

/**
 * Created by bryan on 8/10/15.
 */
public class HadoopConfigurationsController extends AbstractXulEventHandler {
  public static final String HADOOP_CONFIGURATIONS_CONTROLLER = "hadoopConfigurationsController";
  private XulDomContainer xulDomContainer;
  private JfaceMenupopup hadoopConfigurationPopup;

  public HadoopConfigurationsController() {
    setName( HADOOP_CONFIGURATIONS_CONTROLLER );
  }

  public void init() throws XulException {
    try {
      hadoopConfigurationPopup = (JfaceMenupopup) document.getElementById( "hadoop-configuration-popup" );
      Object object = hadoopConfigurationPopup.getManagedObject();
      if ( object instanceof MenuManager ) {
        MenuManager managedObject = (MenuManager) object;
        managedObject.addMenuListener( new IMenuListener() {
          @Override public void menuAboutToShow( IMenuManager iMenuManager ) {
            refresh();
          }
        } );
      }
      refresh();
    } catch ( ClassCastException e ) {
      throw new XulException( e );
    }
  }

  public void setActiveShim( String shim ) throws ConfigurationException {
    HadoopConfigurationBootstrap.getInstance().setActiveShim( shim );
    refresh();
  }

  public void refresh() {
    try {
      hadoopConfigurationPopup.removeChildren();
      for ( HadoopConfigurationInfo hadoopConfigurationInfo : HadoopConfigurationBootstrap.getInstance()
        .getHadoopConfigurationInfos() ) {
        XulMenuitem menuitem = hadoopConfigurationPopup.createNewMenuitem();
        menuitem.setId( "hadoop-configuration-" + hadoopConfigurationInfo.getId() );
        menuitem.setLabel( hadoopConfigurationInfo.getName() );
        if ( hadoopConfigurationInfo.isActive() ) {
          menuitem.setImage( "ui/images/true.png" );
        } else if ( hadoopConfigurationInfo.isWillBeActiveAfterRestart() ) {
          menuitem.setImage( "ui/images/reset_option.png" );
        }
        menuitem.setCommand(
          HADOOP_CONFIGURATIONS_CONTROLLER + ".setActiveShim('" + hadoopConfigurationInfo.getId() + "')" );
      }
    } catch ( KettleException e ) {
      e.printStackTrace();
    } catch ( ConfigurationException e ) {
      e.printStackTrace();
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }
}
