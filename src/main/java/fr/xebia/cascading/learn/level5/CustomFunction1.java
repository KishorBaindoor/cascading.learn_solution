package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;



/**
 * Created by kishorb on 7/3/2015.
 */
public class CustomFunction1 extends BaseOperation implements Function {

    public CustomFunction1(Fields idf){
        super(1, new Fields("TF-IDF"));
    }
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        // get the arguments TupleEntry
        TupleEntry arguments = functionCall.getArguments();
        // create a Tuple to hold our result values
        Tuple result = new Tuple();
        result.add(Double.toString (arguments.getDouble("TF")* java.lang.Math.log(5.00 / arguments.getLong("countDocumentsEachWords"))));
        // insert some values into the result Tuple
        // return the result Tuple
        functionCall.getOutputCollector().add(result);
    }
}
