package com.actian.services.dataflow.operators;

import com.pervasive.datarush.graphs.LogicalSubgraph;
import com.pervasive.datarush.operators.CompositionIterator;
import com.pervasive.datarush.operators.IterativeExecutionContext;
import com.pervasive.datarush.operators.IterativeMetadataContext;
import com.pervasive.datarush.operators.IterativeOperator;
import com.pervasive.datarush.operators.MetadataContext;
import com.pervasive.datarush.operators.OperatorComposable;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.group.Aggregation;
import com.pervasive.datarush.operators.group.Group;
import com.pervasive.datarush.operators.record.DeriveFields;
import com.pervasive.datarush.operators.record.FieldDerivation;
import com.pervasive.datarush.operators.sink.CollectRecords;
import com.pervasive.datarush.ports.record.DataOrdering;
import com.pervasive.datarush.ports.record.MetadataUtil;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.ports.record.UnspecifiedPartialDistribution;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.record.RecordValued;
import com.pervasive.datarush.tokens.scalar.DoubleValued;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.TypeUtil;
import com.pervasive.datarush.util.PropertyUtil;
import static com.pervasive.datarush.types.TokenTypeConstant.DOUBLE;
import static com.pervasive.datarush.types.TokenTypeConstant.record;

import com.pervasive.datarush.analytics.functions.StatsFunctions;

public final class ZScore extends IterativeOperator implements RecordPipelineOperator {
    
    //required: declare any input ports
    private final RecordPort input = newRecordInput("input");
    //required: declare any output ports
    private final RecordPort output = newRecordOutput("output");
    //required: declare any properties
    private String fieldName;
 
    /**
     * Default constructor.
     */
    //required: all operators must have a default constructor
    public ZScore() {
    }
 
    /**
     * Convenience constructor, specifies the field for which to
     * calculate the z-score
     * @param fieldName the field for which to calculate the z-score
     */
    //optional: convenience constructor
    public ZScore(String fieldName) {
        setFieldName(fieldName);
    }
 
    ///////////////////////////////////////////////////
    //
    // Required: getters and setters for each property
    //
    ///////////////////////////////////////////////////
    /**
     * Returns the field name of field for which to calculate the z-score
     * @return the field for which to calculate the z-score
     */
    public String getFieldName() {
        return fieldName;
    }
 
    /**
     * Sets the field name of field for which to calculate the z-score
     * @param fieldName the field for which to calculate the z-score
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
 
    ///////////////////////////////////////////////////
    //
    // Required: getters for each port
    //
    ///////////////////////////////////////////////////
    /**
     * Returns the input port
     * @return the input port.
     */
    @Override
    public RecordPort getInput() {
        return input;
    }
 
    /**
     * Returns the output port containing input data plus an additional
     * field <fieldName>_zscore containing the calculated zscore for the given
     * input field
     * @return the output port
     */
    @Override
    public RecordPort getOutput() {
        return output;
    }
 
    @Override
    protected void computeMetadata(IterativeMetadataContext ctx) {
 
        //best practice: perform validation first
        // make sure field is non-null
        PropertyUtil.checkNotEmpty(fieldName, "fieldName");
 
        // make sure field exists in the input
        input.getType(ctx).verifyNames(fieldName);
 
        //Required: declare parallelizability of this operator, and each input and output port
        //In this case, we can use the convenience method. This method does the following:
        // 1)Specifies that the input will be accessed iteratively in parallel (distributed)
        // 2)Specifies that the output record port is parallel (distributed)
        // 3)Specifies that our parallelism is to be determined by the source parallelism
        MetadataUtil.negotiateParallelismBasedOnSourceAssumingParallelizableRecords(ctx);
 
        //required: declare sort order and data distribution of each
        //  input port. data is staged *once* according to these parameters and each
        //  iteration then uses that dataset. incorrectly defining these parameters
        //  means that data may be redistributed with each iteration
        //
        //  in this case, we want input to be staged in a parallel fashion since both the Group and the DeriveFields
        //  operators that we use during execution are parallelizable.
        //
        //  in this case, we don't care about distribution or ordering since neither Group nor DeriveFields care
        input.setRequiredDataDistribution(ctx, UnspecifiedPartialDistribution.INSTANCE);
        input.setRequiredDataOrdering(ctx, DataOrdering.UNSPECIFIED);
 
        //required: declare each output type
        //
        //  in this case, outputType is input plus the new column appended
        RecordTokenType outputType = TypeUtil.mergeTypes(input.getType(ctx), record(DOUBLE(outputFieldName())));
        output.setType(ctx, outputType);
 
        //best practice: declare output data ordering and distribution if possible
        //
        //  in this case, since we're merely appending a field, output data ordering
        //  and partitioning are the same as input. this is consistent with the
        //  metadata produce by DeriveFields
        output.setOutputDataOrdering(ctx, input.getSourceDataOrdering(ctx));       
        output.setOutputDataDistribution(ctx, input.getSourceDataDistribution(ctx));
    }
 
    @Override
    protected CompositionIterator createIterator(MetadataContext ctx) {
        return new ZScoreIterator();
    }
 
    /**
     * required: iterator -- this is the master execution piece that
     * handles execution of subgraphs. CompositionIterators are normally
     * inner classes of the primary operator since they always need to
     * reference input and output ports
     */
    private class ZScoreIterator implements CompositionIterator {
 
        //constants
        private static final String FIELD_STDDEV = "stddev";
        private static final String FIELD_AVG = "avg";
 
        //required: iteration state. all iterators
        // have some internal state that they capture in their
        // primary *execute* method and then reference in *finalComposition*
        private double avg;
        private double stddev;
 
        public ZScoreIterator() {
            super();
        }
 
        @Override
        public void execute(IterativeExecutionContext ctx) {           
 
            //create a new subgraph (typically execute methods
            //will create one or more subgraphs)
            LogicalSubgraph sub = ctx.newSubgraph();
 
            //add a group operator to calculate mean and stddev
            Group group = sub.add(new Group());
            group.setAggregations(new Aggregation[]{Aggregation.avg(ZScore.this.fieldName).as(FIELD_AVG),
                    Aggregation.stddev(ZScore.this.fieldName,true).as(FIELD_STDDEV)});
 
            //create a connection in the subgraph from the main input to the Group input
            sub.connect(ZScore.this.input, group.getInput());
 
            //create a placeholder to capture the results
            CollectRecords placeholder = sub.add(new CollectRecords());
            sub.connect(group.getOutput(), placeholder.getInput());
 
            //run the subgraph
            sub.run();
 
            //extract avg and stddev from results (results from
            //Group consist of a single row of two fields "stddev" and "avg")
            RecordTokenList list = placeholder.getOutput();
            assert list.size() == 1;
            RecordValued row = list.getToken(0);                  
            this.avg = ((DoubleValued)row.getField(FIELD_AVG)).asDouble();
            this.stddev = ((DoubleValued)row.getField(FIELD_STDDEV)).asDouble();
        }
 
        @Override
        public void finalComposition(OperatorComposable ctx) {
 
            //create a DeriveFields to derive the zscore
            FieldDerivation zscore =
                FieldDerivation.derive(outputFieldName(),
                        StatsFunctions.zscore(ZScore.this.fieldName, this.avg, this.stddev));
            DeriveFields convert =
                ctx.add(new DeriveFields(zscore));
 
            //connect from the main operators's input to the converter's input
            ctx.connect(ZScore.this.input, convert.getInput());
 
            //required: implementations of finalComposition must connect
            //  all output ports
            ctx.connect(convert.getOutput(), ZScore.this.output);
        }
 
    }
 
    private String outputFieldName() {
        return fieldName + "_zscore";
    }
}
