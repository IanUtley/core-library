package com.actian.services.dataflow.operators;

import org.apache.commons.math.linear.MatrixUtils;
import org.apache.commons.math.linear.RealMatrix;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonProperty;

import com.pervasive.datarush.DRException;
import com.pervasive.datarush.annotations.OperatorDescription;
import com.pervasive.datarush.annotations.PortDescription;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.MetadataContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.RecordSourceOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.record.BalancedDistribution;
import com.pervasive.datarush.ports.record.DataOrdering;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.tokens.scalar.IntSettable;
import com.pervasive.datarush.types.Field;
import com.pervasive.datarush.types.TokenTypeConstant;

@JsonAutoDetect({org.codehaus.jackson.annotate.JsonMethod.NONE})
@OperatorDescription("A discrete-time Markov chain implementation.")
public class MarkovChain
  extends ExecutableOperator
  implements RecordSourceOperator
{
	
  private final RecordPort output = newRecordOutput("state");
  private RealMatrix transitions = MatrixUtils.createRealIdentityMatrix(2);
  private int steps = 1;
  private int state = 0;
  
  @PortDescription("The resultant Markov chain.")
  public RecordPort getOutput()
  {
    return this.output;
  }
  
  protected void execute(ExecutionContext ctx)
  {
    RecordOutput output = this.output.getOutput(ctx);
    
    IntSettable indexField = (IntSettable)output.getField(0);
    IntSettable lastField = (IntSettable)output.getField(1);
    IntSettable stateField = (IntSettable)output.getField(2);
    
    int numStates = this.transitions.getColumnDimension();
    
    int nodeId = ctx.getPartitionInfo().getPartitionID();
    for (int i = 1; i <= this.steps; i++)
    {
      double value = Math.random();
      for (int j = 0; j < numStates; j++)
      {
        value -= this.transitions.getEntry(this.state, j);
        if (value <= 0.0D)
        {
          indexField.set(i);
          lastField.set(this.state);
          this.state = j;
          stateField.set(this.state);
          output.push();
          break;
        }
      }
    }
    output.pushEndOfData();
  }
  
  protected void computeMetadata(StreamingMetadataContext ctx)
  {
    validateMatrix(ctx);
    
    ctx.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);
    this.output.setType(ctx, TokenTypeConstant.record(new Field[] { TokenTypeConstant.INT("index"), TokenTypeConstant.INT("last_state"), TokenTypeConstant.INT("state") }));
    this.output.setOutputDataDistribution(ctx, BalancedDistribution.INSTANCE);
    this.output.setOutputDataOrdering(ctx, DataOrdering.UNSPECIFIED);
  }
  
  private void validateMatrix(MetadataContext ctx)
  {
    if (this.transitions == null) {
      throw new DRException("Transistion matrix is not set.");
    }
    if (!this.transitions.isSquare()) {
      throw new DRException("Transistion matrix is not square.");
    }
    for (int i = 0; i < this.transitions.getRowDimension(); i++)
    {
      double sum = 0.0D;
      for (int j = 0; j < this.transitions.getColumnDimension(); j++) {
        sum += this.transitions.getEntry(i, j);
      }
      if (Math.abs(sum - 1.0D) > 1.E-005D) {
        throw new DRException("Row " + (i + 1) + " of matrix does not sum to 1.");
      }
    }
  }
  
  @JsonProperty("steps")
  public int getSteps()
  {
    return this.steps;
  }
  
  public void setSteps(int steps)
  {
    this.steps = steps;
  }
  
  @JsonProperty("state")
  public int getState()
  {
    return this.state;
  }
  
  public void setState(int state)
  {
    this.state = state;
  }
  
  public RealMatrix getTransitions()
  {
    return this.transitions;
  }
  
  @JsonProperty("data")
  public double[][] getData()
  {
    return this.transitions.getData();
  }
  
  public void setTransitions(RealMatrix transitions)
  {
    this.transitions = transitions;
  }
  
  public void setData(double[][] data)
  {
    this.transitions = MatrixUtils.createRealMatrix(data);
  }
  
  public void setTransistions(String data)
  {
    String[] parts = data.split(",");
    int length = parts.length;
    if (length <= 2) {
      throw new IllegalArgumentException("Matrix definition is not well formed.");
    }
    double size = Math.sqrt(length);
    double rint = Math.rint(size);
    if (size != rint) {
      throw new IllegalArgumentException("Matrix definition is not square.");
    }
    int l = (int)rint;
    double[][] vals = new double[l][l];
    
    int cell = 0;
    for (String value : parts)
    {
      double d = Double.parseDouble(value);
      vals[(cell / l)][(cell % l)] = d;
      cell++;
    }
    setData(vals);
  }
}
