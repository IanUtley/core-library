package com.actian.services.knime.core.operators;

import com.actian.services.dataflow.operators.MarkovChain;
import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.math.linear.MatrixUtils;
import org.apache.commons.math.linear.RealMatrix;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

final class MarkovChainNodeSettings
  extends AbstractDRSettingsModel<MarkovChain>
{
  private static final int DEFAULT_STATE = new MarkovChain().getState();
  private static final int DEFAULT_STEPS = new MarkovChain().getSteps();
  public final SettingsModelIntegerBounded state = new SettingsModelIntegerBounded("state", DEFAULT_STATE, 0, 2147483647);
  public final SettingsModelIntegerBounded steps = new SettingsModelIntegerBounded("steps", DEFAULT_STEPS, 0, 2147483647);
  public final SettingsModelString matrix = new SettingsModelString("matrix", "1,0\n0,1");
  
  protected List<SettingsModel> getComponentSettings()
  {
    return Arrays.asList(new SettingsModel[] { this.state, this.steps, this.matrix });
  }
  
  public void configure(PortMetadata[] inputTypes, MarkovChain operator)
    throws InvalidSettingsException
  {
    operator.setState(this.state.getIntValue());
    operator.setSteps(this.steps.getIntValue());
    String[] rows = this.matrix.getStringValue().split("\n");
    
    RealMatrix matrix = MatrixUtils.createRealMatrix(rows.length, rows.length);
    
    int x = 0;
    for (String row : rows)
    {
      String[] vals = row.split(",");
      int y = 0;
      for (String val : vals)
      {
        matrix.setEntry(x, y, Double.parseDouble(val));
        y++;
      }
      x++;
    }
    operator.setTransitions(matrix);
  }
}

