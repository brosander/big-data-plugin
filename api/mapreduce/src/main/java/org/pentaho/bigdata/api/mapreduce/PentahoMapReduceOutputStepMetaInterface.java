package org.pentaho.bigdata.api.mapreduce;

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.List;

/**
 * Created by bryan on 1/12/16.
 */
public interface PentahoMapReduceOutputStepMetaInterface extends StepMetaInterface {
  void checkPmr( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev);
}
