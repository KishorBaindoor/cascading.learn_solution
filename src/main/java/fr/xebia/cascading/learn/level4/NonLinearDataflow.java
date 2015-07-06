package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * presidentsSource field(s) : "year","president"
	 * partiesSource field(s) : "year","party"
	 * sink field(s) : "president","party"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s03.html
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {
		Fields common1= new Fields("year");
		Fields common2 = new Fields("year");

		Pipe parties = new Pipe("Parties");
		Pipe presidents = new Pipe("Presidents");
		Pipe Joined1 = new CoGroup(presidents, common1 ,parties, common2, new Fields("year","president","year1","party"), new InnerJoin() );
		Pipe Joined = new Discard(Joined1, new Fields("year","year1"));
		return FlowDef.flowDef()
				.addSource(parties, partiesSource)
				.addSource(presidents, presidentsSource)
				.addTail(Joined)
				.addSink(Joined, sink);

	}
	
	/**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * source field(s) : "president","party"
	 * gaullistSink field(s) : "president","party"
	 * republicanSink field(s) : "president","party"
	 * socialistSink field(s) : "president","party"
	 * 
	 * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s07.html
	 */
	public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {

		Pipe mainPipe = new Pipe("presidentParty");
		ExpressionFilter expGaullist = new ExpressionFilter("!party.equals(\"Gaullist\")",String.class);
		ExpressionFilter expRepublican = new ExpressionFilter("!party.equals(\"Republican\")", String.class);
		ExpressionFilter expSocialist = new ExpressionFilter("!party.equals(\"Socialist\")", String.class);

		Pipe gaullist = new Pipe ("gaullist",mainPipe);
		Pipe Republican = new Pipe("Republican", mainPipe);
		Pipe Socialist = new Pipe ("Socialist", mainPipe);

		gaullist = new Each(gaullist, new Fields("president","party"),expGaullist);
		Republican = new Each(Republican, new Fields("president","party"), expRepublican ) ;
		Socialist = new Each (Socialist, new Fields("president","party"), expSocialist);

		return FlowDef.flowDef()
				.addSource(mainPipe, source)
				.addTail(gaullist)
				.addTail(Republican)
				.addTail(Socialist)
				.addSink(gaullist, gaullistSink)
				.addSink(Republican, republicanSink)
				.addSink(Socialist, socialistSink);


	}
	
}
