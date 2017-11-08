package com.actian.services.dataflow.operators;

import static com.pervasive.datarush.types.TokenTypeConstant.DOUBLE;
import static com.pervasive.datarush.types.TokenTypeConstant.record;

import org.junit.Test;

import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.operators.sink.LogRows;
import com.pervasive.datarush.operators.source.EmitRecords;
import com.pervasive.datarush.sequences.record.RecordTokenList;
import com.pervasive.datarush.tokens.record.RecordToken;
import com.pervasive.datarush.tokens.scalar.DoubleToken;
import com.pervasive.datarush.types.RecordTokenType;

public class ZScoreTest {
    
    public ZScoreTest() {
    }

    @Test
    public void testZScore() {
        
        LogicalGraph g = LogicalGraphFactory.newLogicalGraph("test_zscore");
        EmitRecords er = g.add(new EmitRecords());
        RecordTokenType dataType = record(DOUBLE("x"));
        RecordTokenList data = new RecordTokenList(dataType, 4);
        data.append(new RecordToken(dataType, DoubleToken.parse("1.4")));
        data.append(new RecordToken(dataType, DoubleToken.parse("5.3")));
        data.append(new RecordToken(dataType, DoubleToken.parse("19.2")));
        data.append(new RecordToken(dataType, DoubleToken.parse("6.4")));
        er.setInput(data);

        ZScore zs = g.add(new ZScore());
        zs.setFieldName("x");

        /*
        RecordTokenList expectedData = new RecordTokenList(
            record(STRING("id"),STRING("value"),INT("offset"),
                   STRING("id_2"),STRING("value_2"),INT("offset_2"),
                   STRING("id_3"),STRING("value_3"),INT("offset_3")),4);

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id1"),StringToken.parse("apple"),IntToken.parse("1"),
            StringToken.NULL,StringToken.NULL,IntToken.NULL,
            StringToken.parse("id1"),StringToken.parse("cherry"),IntToken.parse("3")));

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id1"),StringToken.parse("cherry"),IntToken.parse("3"),
            StringToken.parse("id1"),StringToken.parse("apple"),IntToken.parse("1"),
            StringToken.parse("id1"),StringToken.parse("date"),IntToken.parse("4")));

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id1"),StringToken.parse("date"),IntToken.parse("4"),
            StringToken.parse("id1"),StringToken.parse("cherry"),IntToken.parse("3"),
            StringToken.NULL,StringToken.NULL,IntToken.NULL));

        expectedData.append(new RecordToken(expectedData.getType(),
            StringToken.parse("id2"),StringToken.parse("apricot"),IntToken.parse("1"),
            StringToken.NULL,StringToken.NULL,IntToken.NULL,
            StringToken.NULL,StringToken.NULL,IntToken.NULL));

        EmitRecords expectedRecords = g.add(new EmitRecords(expectedData));
        AssertEqual p = g.add(new AssertEqual());

        g.connect(ll.getOutput(), p.getActualInput());
        g.connect(expectedRecords.getOutput(),p.getExpectedInput());
        */
        
        LogRows lr = g.add(new LogRows(1));
        g.connect(er.getOutput(), zs.getInput());
        g.connect(zs.getOutput(), lr.getInput());
        // Compile & run
        g.run(EngineConfig.engine().parallelism(1));
    }
    
}
