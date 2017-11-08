package com.actian.services.dataflow.operators;

import static com.pervasive.datarush.types.TokenTypeConstant.STRING;
import static com.pervasive.datarush.types.TokenTypeConstant.record;

import org.junit.Test;

import com.pervasive.datarush.analytics.decisiontree.predictor.DecisionTreePredictor;
import com.pervasive.datarush.analytics.pmml.ReadPMML;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.operators.source.EmitRecords;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.record.RecordToken;
import com.pervasive.datarush.tokens.scalar.StringToken;
import com.pervasive.datarush.types.RecordTokenType;

public class Scratch {

	public Scratch() {
		
	}
	
	@Test
	public void testScratch() {
		System.out.println("This is a test");
		
		LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_model");
		EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(
        		STRING("field1"), STRING("field2"), STRING("field3"), STRING("field4"), STRING("field5"),
        		STRING("field6"), STRING("field7"), STRING("field8"), STRING("field9"), STRING("field10"),
        		STRING("field11"), STRING("field12"), STRING("field13"), STRING("field14"), STRING("field15"),
        		STRING("field16"), STRING("field17"), STRING("field18"), STRING("field19"), STRING("field20")
        		);
        RecordTokenList data = new RecordTokenList(dataType, 1);
        data.append(new RecordToken(dataType, StringToken.parse("b"), StringToken.parse("f"),StringToken.NULL));
        er.setInput(data);
        
		// Create a Discrete
		DecisionTreePredictor pred = new DecisionTreePredictor();
		ReadPMML pmmlReader = new ReadPMML("/Users/appledev/R/XGBoostExample/Mushrooms/decision.pmml");
		g.add(pred);
		g.add(pmmlReader);
		g.connect(pmmlReader.getOutput(), pred.getModel());
		
		LogRows lr = new LogRows(1);
		g.add(lr);
		
		g.connect(er.getOutput(),pred.getInput());
		g.connect(pred.getOutput(), lr.getInput());
		
		g.run();
	}
	
}
