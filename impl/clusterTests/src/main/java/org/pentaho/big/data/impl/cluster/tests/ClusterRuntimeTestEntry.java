/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.impl.cluster.tests;

import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.impl.HelpUrlPayload;
import org.pentaho.runtime.test.action.impl.RuntimeTestActionImpl;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

/**
 * This is a convenience class that will add a shim troubleshooting guide action if none is specified and the severity
 * is >= WARNING
 */
public class ClusterRuntimeTestEntry extends RuntimeTestResultEntryImpl {
  private static final Class<?> PKG = ClusterRuntimeTestEntry.class;

  public ClusterRuntimeTestEntry( MessageGetterFactory messageGetterFactory, RuntimeTestEntrySeverity severity,
                                  String description, String message ) {
    this( messageGetterFactory, severity, description, message, null );
  }

  public ClusterRuntimeTestEntry( RuntimeTestEntrySeverity severity, String description, String message,
                                  RuntimeTestAction runtimeTestAction ) {
    this( severity, description, message, null, runtimeTestAction );
  }

  public ClusterRuntimeTestEntry( MessageGetterFactory messageGetterFactory, RuntimeTestEntrySeverity severity,
                                  String description, String message,
                                  Throwable exception ) {
    this( severity, description, message, exception, createDefaultAction( messageGetterFactory, severity ) );
  }

  public ClusterRuntimeTestEntry( RuntimeTestEntrySeverity severity, String description, String message,
                                  Throwable exception,
                                  RuntimeTestAction runtimeTestAction ) {
    super( severity, description, message, exception, runtimeTestAction );
  }

  private static RuntimeTestAction createDefaultAction( MessageGetterFactory messageGetterFactory,
                                                        RuntimeTestEntrySeverity severity ) {
    if ( severity.ordinal() >= RuntimeTestEntrySeverity.WARNING.ordinal() ) {
      return new RuntimeTestActionImpl( messageGetterFactory.create( PKG ).getMessage(
        "RuntimeTestResultEntryWithDefaultShimHelp.TroubleshootingGuide" ),
        "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc", severity, new HelpUrlPayload( messageGetterFactory,
          messageGetterFactory.create( PKG ).getMessage( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Title" ),
          messageGetterFactory.create( PKG ).getMessage( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Header" ),
          messageGetterFactory.create( PKG ).getMessage( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc" ) ) );
    }
    return null;
  }
}
