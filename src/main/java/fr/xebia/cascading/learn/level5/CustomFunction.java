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
public class CustomFunction extends BaseOperation implements Function {

    public CustomFunction(Fields fields){
        super(2, new Fields("TF"));
    }
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        // get the arguments TupleEntry
        TupleEntry arguments = functionCall.getArguments();
        // create a Tuple to hold our result values
        Tuple result = new Tuple();
        result.add( Double.valueOf(arguments.getLong("counts")/ arguments.getLong("MaxValueInDoc")));
        // insert some values into the result Tuple
        // return the result Tuple
        functionCall.getOutputCollector().add(result);
    }
}
