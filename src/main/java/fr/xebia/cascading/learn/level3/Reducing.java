package fr.xebia.cascading.learn.level3;

import cascading.flow.FlowDef;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Once each input has been individually curated, it can be needed to aggregate information.
 */
public class Reducing {
	
	/**
	 * {@link GroupBy} "word" and then apply {@link Count}. It should be noted
	 * that once grouped, the semantic is different. You will need to use a
	 * {@link Every} instead of a {@link Each}. And {@link Count} is an
	 * {@link Aggregator} instead of a {@link Function}.
	 * 
	 * source field(s) : "word"
	 * sink field(s) : "word","count"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s03.html
	 */
	public static FlowDef aggregate(Tap<?, ?, ?> source, Tap<?, ?, ?> sink)
	{


		Pipe pipe = new Pipe("wordsFile");
		pipe = new GroupBy(pipe, new Fields("words"));
		//pipe = new GroupBy(pipe, new Fields("word"),Fields.ALL);
		pipe = new Every (pipe, new Fields("words"), new Count(new Fields("wordcount")),new Fields("words","wordcount"));
		return FlowDef.flowDef()//
				.addSource(pipe, source) //
				.addTail(pipe)//
				.addSink(pipe, sink);

	}


	/**
	 * Aggregation should be done as soon as possible and Cascading does have a technique almost similar to map/reduce 'combiner'.
	 * Use {@link CountBy} in order to do the same thing as above. It is shorter to write and more efficient.
	 * 
	 * source field(s) : "word"
	 * sink field(s) : "word","count"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s08.html
	 */
	public static FlowDef efficientlyAggregate(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		Pipe pipe = new Pipe("count-words");
		Fields groupingField = new Fields("word");
		Fields wCount = new Fields("count");
		CountBy countby = new CountBy(wCount);
		pipe = new AggregateBy(pipe, groupingField, countby );
		return FlowDef.flowDef()//
				.addSource(pipe, source) //
				.addTail(pipe)//
				.addSink(pipe, sink);


	}
}
