package com.actian.services.knime.core.operators;

import com.actian.services.dataflow.operators.MarkovChain;
import com.pervasive.datarush.knime.core.framework.AbstractDRNodeFactory;
import com.pervasive.datarush.knime.core.framework.DRNodeModel;

public final class MarkovChainNodeModelFactory
  extends AbstractDRNodeFactory<MarkovChain>
{
  protected MarkovChainNodeDialogPane createNodeDialogPane()
  {
    return new MarkovChainNodeDialogPane();
  }
  
  public DRNodeModel<MarkovChain> createDRNodeModel()
  {
    return new DRNodeModel(new MarkovChain(), new MarkovChainNodeSettings());
  }
  
  protected boolean hasDialog()
  {
    return true;
  }
}
