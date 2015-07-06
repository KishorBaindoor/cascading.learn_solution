package fr.xebia.cascading.learn.level0;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Level0 is a very basic example of how to do a copy. It should provide you
 * with enough leads about where to look for more information. You will need to
 * complete later levels in order to make the tests pass.
 *
 * @see http://docs.cascading.org/cascading/2.5/userguide/html/
 * @see http://docs.cascading.org/cascading/2.5/javadoc/
 */
public class PlainCopy {

	/**
	 * A copy is a job with an empty set of operations.
	 *
	 * source field(s) : "line"
	 * sink field(s) : "line"
	 */
	public static FlowDef createFlowDefUsing(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		Pipe pipe = new Pipe("plainCopy");
		//pipe = new Discard(pipe, new Fields("line"));

		return FlowDef.flowDef()//
				.addSource(pipe, source) //
				.addTail(pipe)//
				.addSink(pipe, sink);
	}

}