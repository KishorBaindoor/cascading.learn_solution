package fr.xebia.cascading.learn.level6;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.assertion.AssertMatchesAll;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
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
	public static void main(String[] args)  throws Exception{


		String sourcePath = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\src\\test\\resources\\fifth-france-parties.txt";
			Tap<?,?,?> source = new FileTap(new TextDelimited(true, "\t"), sourcePath);

			String targetPath = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\target\\level2\\dummy.txt";
			Tap<?,?,?> sink = new FileTap(new TextDelimited(true, "\t"), targetPath);

			Pipe pipe = new Pipe("plain Copy");
			ExpressionFilter filter = new ExpressionFilter("year.equals(\"1958\")", String.class);
			pipe = new Each(pipe, new Fields("year"), filter);
			pipe = new Each( pipe, DebugLevel.VERBOSE, new Debug() );
			FlowDef flowdef =  FlowDef.flowDef()
					.addSource(pipe, source)
					.addTail(pipe)
					.addSink(pipe, sink);
		flowdef.setDebugLevel( DebugLevel.VERBOSE );
			new LocalFlowConnector().connect(flowdef).complete();

		experimentWithSubAssemblies();
		experimentWithTheAssertions();
	}

	/*
	 * {@link Assertion}s are also useful a concept, even in cascading.
	 *
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s02.html
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch09s09.html
	 */



	public static void experimentWithTheAssertions() {
		//throw new UnsupportedOperationException("Go back and try it on your own.");



			String sourcePath = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\src\\test\\resources\\fifth-france-parties.txt";
			Tap<?,?,?> source = new FileTap(new TextDelimited(true, "\t"), sourcePath);

			String targetPath = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\target\\level2\\dummy1.txt";
			Tap<?,?,?> sink = new FileTap(new TextDelimited(true, "\t"), targetPath);

			Pipe pipe = new Pipe("plain Copy");
			ExpressionFilter filter = new ExpressionFilter("year.equals(\"1958\")", String.class);
			pipe = new Each(pipe, new Fields("year"), filter);

		AssertMatchesAll matches = new AssertMatchesAll("(Gaullist|Republican||Socialist)");
		pipe = new Each(pipe, new Fields("party"), AssertionLevel.STRICT, matches );
			FlowDef flowdef =  FlowDef.flowDef()
					.addSource(pipe, source)
					.addTail(pipe)
					.addSink(pipe,sink)
					.setAssertionLevel(AssertionLevel.STRICT);
			flowdef.setDebugLevel( DebugLevel.VERBOSE );
			new LocalFlowConnector().connect(flowdef).complete();



		}

	/**
	 * Last but not least is the concept of {@link SubAssembly} which allows
	 * to package your common flow operations in a more concise way.
	 * You already have been using them, look at the class hierarchy and
	 * the code of {@link Discard}.
	 *
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08.html
	 */
	public static void experimentWithSubAssemblies() {
		//throw new UnsupportedOperationException("Go back and try it on your own.");

		String sourcePath1 = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\src\\test\\resources\\fifth-france-parties.txt";
		Tap<?,?,?> source1 = new FileTap(new TextDelimited(true, "\t"), sourcePath1);

		String sourcePath2 = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\src\\test\\resources\\fifth-france-presidents.txt";
		Tap<?,?,?> source2 = new FileTap(new TextDelimited(true, "\t"), sourcePath2);

		String targetPath = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\target\\level2\\subassembly1.txt";
		Tap<?,?,?> sink1 = new FileTap(new TextDelimited(true, "\t"), targetPath);

		String targetPath2 = "C:\\Users\\kishorb\\gitProjects\\cascading.learn_solution\\target\\level2\\subassembly2.txt";
		Tap<?,?,?> sink2 = new FileTap(new TextDelimited(true, "\t"), targetPath2);

		Pipe parties = new Pipe("Parties");
		Pipe presidents = new Pipe("Presidents");


		SubAssembly out = new SomeAssembly(parties, presidents);

		Pipe split1 = new Pipe("Split1", out.getTails()[0]);
		Pipe split2 = new Pipe("Split2", out.getTails()[1]);
				FlowDef flowdef =  FlowDef.flowDef()
				.addSource(parties, source1)
				.addSource(presidents, source2)
				.addTail(split1)
				.addTail(split2)
				.addSink(split1, sink1)
		        .addSink(split2,sink2);



		new LocalFlowConnector().connect(flowdef).complete();


	}

}
