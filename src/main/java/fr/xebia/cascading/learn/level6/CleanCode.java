package fr.xebia.cascading.learn.level6;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Assertion;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * That's the end. Hope you enjoyed it. But before leaving, 
 * here are a few tips that could help you in the future.
 */
public class CleanCode {

	/**
	 * The {@link Debug} is a pretty useful to understand the flow of data at a specific point.
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch09s02.html
	 */
	public void experimentWithTheDebugFunction() {
		throw new UnsupportedOperationException("Go back and try it on your own.");

		String sourcePath = "src/test/resources/fifth-france-parties.txt";
		String targetPath = "target/level2/filter-with-expression.txt";

		Tap<?,?,?> source = new FileTap(new TextDelimited(true,"\t"),sourcePath );
		Tap<?,?,?> sink = new FileTap(new TextDelimited(true,"\t"), targetPath);

		Pipe pipe = new Pipe("plain Copy");
		pipe = new Each(pipe, new Fields("year"), new ExpressionFilter("year.equals(\"1958\")", String.class), Fields.ALL);
		pipe = new Each( pipe, DebugLevel.VERBOSE, new Debug() );
		FlowDef flowdef = new FlowDef.flowDef()
				.addSource(pipe,source)
				.addTail(pipe)
				.addSink(pipe,sink);
		new LocalFlowConnector().connect(flowdef).complete();





	}
	
	/**
	 * {@link Assertion}s are also useful a concept, even in cascading.
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s02.html
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch09s09.html
	 */
	public void experimentWithTheAssertions() {
		throw new UnsupportedOperationException("Go back and try it on your own.");
	}
	
	/**
	 * Last but not least is the concept of {@link SubAssembly} which allows
	 * to package your common flow operations in a more concise way.
	 * You already have been using them, look at the class hierarchy and 
	 * the code of {@link Discard}.
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08.html
	 */
	public void experimentWithSubAssemblies() {
		throw new UnsupportedOperationException("Go back and try it on your own.");
	}
	
}
