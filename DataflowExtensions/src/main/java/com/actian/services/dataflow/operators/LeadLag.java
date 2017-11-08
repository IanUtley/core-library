/*
   Copyright 2015 Actian Corporation
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.actian.services.dataflow.operators;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

import com.pervasive.datarush.DRException;
import com.pervasive.datarush.annotations.OperatorDescription;
import com.pervasive.datarush.annotations.PortDescription;
import com.pervasive.datarush.annotations.PropertyDescription;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarInputField;
import com.pervasive.datarush.ports.record.DataOrdering;
import com.pervasive.datarush.ports.record.MetadataUtil;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.TokenOrder;
import com.pervasive.datarush.tokens.TokenUtils;
import com.pervasive.datarush.tokens.TokenValued;
import com.pervasive.datarush.tokens.record.RecordSettable;
import com.pervasive.datarush.tokens.record.SortKey;
import com.pervasive.datarush.tokens.scalar.DoubleRegister;
import com.pervasive.datarush.tokens.scalar.DoubleValued;
import com.pervasive.datarush.tokens.scalar.FloatRegister;
import com.pervasive.datarush.tokens.scalar.FloatValued;
import com.pervasive.datarush.tokens.scalar.IntRegister;
import com.pervasive.datarush.tokens.scalar.IntValued;
import com.pervasive.datarush.tokens.scalar.LongRegister;
import com.pervasive.datarush.tokens.scalar.LongValued;
import com.pervasive.datarush.tokens.scalar.NumericRegister;
import com.pervasive.datarush.tokens.scalar.NumericValued;
import com.pervasive.datarush.tokens.scalar.ScalarRegister;
import com.pervasive.datarush.tokens.scalar.ScalarSettable;
import com.pervasive.datarush.tokens.scalar.StringRegister;
import com.pervasive.datarush.tokens.scalar.StringValued;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.TypeUtil;

@JsonAutoDetect(JsonMethod.NONE)
@JsonTypeName("leadLag")
@OperatorDescription("Extends data with lead and lag values.")
public class LeadLag extends ExecutableOperator implements RecordPipelineOperator {
	
    private final RecordPort input = newRecordInput("input");
    private final RecordPort output = newRecordOutput("output");
    private String[] keys;
    private String valueField;
    private boolean ascending = true;

    @Override
    protected void execute(ExecutionContext ctx) {
        RecordInput inputRec = (RecordInput) ctx.getInputPort(getInput());
        RecordOutput outputRec = (RecordOutput) ctx.getOutputPort(getOutput());

        ScalarRegister[] oldkeyValues = new ScalarRegister[keys.length];
        ScalarRegister[] newkeyValues = new ScalarRegister[keys.length];
        int keyPos = 0;
        for (String key : keys) {
            String ftype = inputRec.getField(key).getType().name();
            switch (ftype) {
                case "INT":
                    IntRegister ireg = new IntRegister();
                    ireg.setNull();
                    oldkeyValues[keyPos] = ireg;
                    ireg = new IntRegister();
                    ireg.setNull();
                    newkeyValues[keyPos] = ireg;
                    break;
                case "STRING":
                    StringRegister sreg = new StringRegister();
                    sreg.setNull();
                    oldkeyValues[keyPos] = sreg;
                    sreg = new StringRegister();
                    sreg.setNull();
                    newkeyValues[keyPos] = sreg;
                    break;
                case "LONG":
                    LongRegister lreg = new LongRegister();
                    lreg.setNull();
                    oldkeyValues[keyPos] = lreg;
                    lreg = new LongRegister();
                    lreg.setNull();
                    newkeyValues[keyPos] = lreg;
                    break;
                case "NUMERIC":
                    NumericRegister nreg = new NumericRegister();
                    nreg.setNull();
                    oldkeyValues[keyPos] = nreg;
                    nreg = new NumericRegister();
                    nreg.setNull();
                    newkeyValues[keyPos] = nreg;
                    break;
                case "FLOAT":
                    FloatRegister freg = new FloatRegister();
                    freg.setNull();
                    oldkeyValues[keyPos] = freg;
                    freg = new FloatRegister();
                    freg.setNull();
                    newkeyValues[keyPos] = freg;
                    break;
                case "DOUBLE":
                    DoubleRegister dreg = new DoubleRegister();
                    dreg.setNull();
                    oldkeyValues[keyPos] = dreg;
                    dreg = new DoubleRegister();
                    dreg.setNull();
                    newkeyValues[keyPos] = dreg;
                    break;
                default:
                    throw new DRException("Cannot handle datatype " + ftype);
            }
            keyPos++;
        }
        RecordTokenList rtl = new RecordTokenList(outputRec.getType(), 2);
        rtl.appendNull(2);
        RecordSettable row1 = rtl.getTokenSetter(0);
        RecordSettable row2 = rtl.getTokenSetter(1);
        boolean markForPush;
        boolean startOfData = true;
        while (inputRec.stepNext()) {
            markForPush = false;
            keyPos = 0;
            for (String key : keys) {
                newkeyValues[keyPos++].set(inputRec.getField(key));
            }
            //TokenUtils.transfer(TokenUtils.selectFields(inputRec, keys), newkeyValues);
            if (!isSameGroup(newkeyValues, oldkeyValues)) {
                // Did we have an old row?
                if (!startOfData) {
                    for (ScalarInputField sif : inputRec.getFields()) {
                        row1.getField(sif.getName() + "_3").setNull();
                    }
                    markForPush = true;
                }
                for (ScalarInputField sif : inputRec.getFields()) {
                    row2.getField(sif.getName()).set(sif);
                    row2.getField(sif.getName() + "_2").setNull();
                }
            } else {
                // Same Group
                for (ScalarInputField sif : inputRec.getFields()) {
                    row1.getField(sif.getName() + "_3").set(sif);
                    row2.getField(sif.getName()).set(sif);
                    row2.getField(sif.getName() + "_2").set((TokenValued) row1.getField(sif.getName()));
                }
                markForPush = true;
            }
            int fpos = 0;
            for (ScalarSettable ss : row1.getFields()) {
                outputRec.getField(fpos++).set((TokenValued) ss);
            }
            if (markForPush) {
                outputRec.push();
            }
            // Housekeeping.
            TokenUtils.transfer(newkeyValues, oldkeyValues);

            fpos = 0;
            for (ScalarSettable ss : row2.getFields()) {
                row1.getField(fpos).set((TokenValued) row2.getField(fpos));
                fpos++;
            }
            startOfData = false;
        }
        if (!startOfData) {
            for (ScalarInputField sif : inputRec.getFields()) {
                row1.getField(sif.getName() + "_3").setNull();
            }
            int fpos = 0;
            for (ScalarSettable ss : row1.getFields()) {
                outputRec.getField(fpos++).set((TokenValued) ss);
            }
            outputRec.push();
        }
        outputRec.pushEndOfData();
    }

    @Override
    protected void computeMetadata(StreamingMetadataContext ctx) {
        RecordTokenType schema = input.getCombinedMetadata(ctx).getType();

        ctx.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);

        if (keys == null || keys.length == 0) {
            throw new DRException("Partition keys must be set.");
        }

        if (valueField == null || valueField.isEmpty()) {
            throw new DRException("Order field must be set.");
        }
        schema = TypeUtil.merge(schema, schema, schema);

        // Do we need to redistribute?
        //DataDistribution inputDistribution = input.getSourceDataDistribution(ctx);
        //input.setRequiredDataDistribution(ctx, KeyDrivenDataDistribution.hashed(keys));
        MetadataUtil.negotiateGrouping(ctx, input, keys);
        //input.setRequiredDataDistribution(ctx, inputDistribution);
        
        SortKey[] sorts = new SortKey[keys.length + 1];
        int keyPos = 0;
        DataOrdering inputOrdering = input.getSourceDataOrdering(ctx);
        // Is in the input sorted already?
        if (inputOrdering.isOrdered(keys)) {
            SortKey[] currentSorts = input.getSourceDataOrdering(ctx).getPrefixOrdering(keys);
            for(SortKey sk : currentSorts) {
                sorts[keyPos] = currentSorts[keyPos];
                keyPos++;
                if (sk.getName().equals(valueField)) {
                    throw new DRException("Field to order by cannot be a partition key.");
                }
            }
        } else {
            for (String key : keys) {
                sorts[keyPos] = new SortKey(key, TokenOrder.ASCENDING);
                keyPos++;
                if (key.equals(valueField)) {
                    throw new DRException("Field to order by cannot be a partition key.");
                }
            }
        }
        sorts[keyPos] = new SortKey(valueField, ascending ? TokenOrder.ASCENDING : TokenOrder.DESCENDING);
        input.setRequiredDataOrdering(ctx, new DataOrdering(sorts));
        
        output.setType(ctx, schema);
        output.setOutputDataDistribution(ctx, input.getCombinedDataDistribution(ctx));
        output.setOutputDataOrdering(ctx, input.getCombinedDataOrdering(ctx));
        
    }

    @Override
    @PortDescription("Input data")
    public RecordPort getInput() {
        return input;
    }

    @Override
    @PortDescription("Data with lead/lag values")
    public RecordPort getOutput() {
        return output;
    }

    @JsonProperty
    public String[] getKeys() {
        return keys;
    }

    @PropertyDescription("Partition keys for data")
    public void setKeys(String[] keys) {
        this.keys = keys;
    }

    @JsonProperty
    @PropertyDescription("Field to order over")
    public String getValueField() {
        return valueField;
    }

    public void setValueField(String valueField) {
        this.valueField = valueField;
    }

    @JsonProperty("orderAscending")
    @PropertyDescription("The order of the value field to indicate lead or lag.")
    public boolean isOrderAscending() {
        return ascending;
    }

    public void setOrderAscending(boolean isascending) {
        this.ascending = isascending;
    }

    private boolean isSameGroup(ScalarSettable[] a, ScalarSettable[] b) {
        for (int i = 0; i < a.length; i++) {
            switch (a[i].getType().name()) {
                case "STRING":
                    if (TokenUtils.compare((StringValued) a[i], (StringValued) b[i]) != 0) {
                        return false;
                    }
                    ;
                    break;
                case "INT":
                    if (TokenUtils.compare((IntValued) a[i], (IntValued) b[i]) != 0) {
                        return false;
                    }
                    ;
                    break;
                case "LONG":
                    if (TokenUtils.compare((LongValued) a[i], (LongValued) b[i]) != 0) {
                        return false;
                    }
                    ;
                    break;
                case "FLOAT":
                    if (TokenUtils.compare((FloatValued) a[i], (FloatValued) b[i]) != 0) {
                        return false;
                    }
                    ;
                    break;
                case "NUMERIC":
                    if (TokenUtils.compare((NumericValued) a[i], (NumericValued) b[i]) != 0) {
                        return false;
                    }
                    ;
                    break;
                case "DOUBLE":
                    if (TokenUtils.compare((DoubleValued) a[i], (DoubleValued) b[i]) != 0) {
                        return false;
                    }
                    ;
                    break;
                default:
                    throw new DRException("Cannot handle datatype " + a[i].getType().name());
            }
        }
        return true;
    }
}
