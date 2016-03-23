package com.pentaho.big.data.bundles.impl.shim.common;

/**
 * Created by bryan on 3/23/16.
 */
public interface ShimBridgingServiceTrackerListener {
  void onRegister( Class iface, Object object );

  void onUnregister( Class iface, Object object );
}
