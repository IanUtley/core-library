package com.actian.services.knime.core.operators;

import com.actian.services.dataflow.operators.MarkovChain;
import com.pervasive.datarush.knime.core.framework.AbstractDRNodeDialogPane;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;

final class MarkovChainNodeDialogPane
  extends AbstractDRNodeDialogPane<MarkovChain>
{
  private final MarkovChainNodeSettings settings = new MarkovChainNodeSettings();
  
  public MarkovChainNodeDialogPane()
  {
    super(new MarkovChain());
    
    DialogComponentNumber state = new DialogComponentNumber(this.settings.state, "Start state", Integer.valueOf(1));
    DialogComponentNumber steps = new DialogComponentNumber(this.settings.steps, "Iterations", Integer.valueOf(100));
    DialogComponentMultiLineString matrix = new DialogComponentMultiLineString(this.settings.matrix, "Probability Matrix", true, 60, 10);
    addDialogComponent(state);
    addDialogComponent(steps);
    addDialogComponent(matrix);
    
    setDefaultTabTitle("Markov Chain Properties");
  }
  
  protected MarkovChainNodeSettings getSettings()
  {
    return this.settings;
  }
  
  protected boolean isMetadataRequiredForConfiguration(int portIndex)
  {
    return true;
  }
}
