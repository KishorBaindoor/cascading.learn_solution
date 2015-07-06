package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Max;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexReplace;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import fr.xebia.cascading.learn.level2.CustomSplitFunction;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {

	/**
	 * Word count is the Hadoop "Hello world" so it should be the first step.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "word","count"
	 */

	//Solution below is not as per expectation, but can be referred to get the logic.
	public static FlowDef countWordOccurences(Tap<?, ?, ?> source, Tap<?, ?, ?> sink)
	{
		Pipe pipe = new Pipe("wordCount");
		String pattern = new String("\\s+");
		RegexSplitGenerator function = new RegexSplitGenerator(new Fields("word"),pattern);
		ExpressionFunction Expfunction = new ExpressionFunction(new Fields("line1"), "line.toLowerCase()",String.class );
		RegexReplace replace = new RegexReplace(new Fields("word"),"([^A-Za-z]+)", "");
		pipe = new Each(pipe, new Fields("line"),Expfunction);

		pipe = new Each(pipe, new Fields("line1"), function, new Fields("word") );
		pipe= new Each(pipe, new Fields("word"),replace);
		pipe = new GroupBy(pipe, new Fields("word"), Fields.ALL);
		pipe = new Every(pipe, new Fields("word"), new Count(new Fields("count")),Fields.ALL);


		return FlowDef.flowDef()
				.addSource(pipe, source)
				.addTail(pipe)
				.addSink(pipe, sink);


	}
	
	/**
	 * Now, let's try a non trivial job : td-idf. Assume that each line is a
	 * document.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "docId","tfidf","word"
	 * 
	 * <pre>
	 * t being a term
	 * t' being any other term
	 * d being a document
	 * D being the set of documents
	 * Dt being the set of documents containing the term t
	 * 
	 * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
	 * 
	 * where
	 * 
	 * tf(t,d) = f(t,d) / max (f(t',d))
	 * ie the frequency of the term divided by the highest term frequency for the same document
	 * 
	 * idf(t, D) = log( size(D) / size(Dt) )
	 * ie the logarithm of the number of documents divided by the number of documents containing the term t 
	 * </pre>
	 * 
	 * Wikipedia provides the full explanation
	 * @see http://en.wikipedia.org/wiki/tf-idf
	 * 
	 * If you are having issue applying functions, you might need to learn about field algebra
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s07.html
	 * 
	 * {@link First} or {@link Max} can be useful for isolating the maximum.
	 * 
	 * {@link HashJoin} can allow to do cross join.
	 * 
	 * PS : Do no think about efficiency, at least, not for a first try.
	 * PPS : You can remove results where tfidf < 0.1
	 */


	//Solution blelow is not correct, but can be referred to get the logic.
	public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink)
	{
		Pipe pipe = new Pipe("words");
		pipe = new Each(pipe, new Fields("content"), new ExpressionFunction(new Fields("content1"),"content.toLowerCase().trim()",String.class ), new Fields("id","content1"));
		RegexSplitGenerator split = new RegexSplitGenerator(new Fields("words"), "\\s+");
		pipe = new Each(pipe, new Fields("content1"),split, new Fields("id","words")) ;

		RegexReplace replace = new RegexReplace(new Fields("word"), "[^A-Za-z]", "");
		pipe = new Each(pipe, new Fields("words"), replace, new Fields("id","word"));

		Pipe wordcount = new GroupBy(pipe, new Fields("id","word"), Fields.ALL);
		wordcount = new Every(wordcount, new Fields("id","word"), new Count(new Fields("counts")), new Fields("id","word","counts"));

		Pipe countDocumentsEachWords = new Pipe("countDocumentsEachWords", wordcount);
		countDocumentsEachWords = new GroupBy(countDocumentsEachWords, new Fields("word"), Fields.ALL);
		countDocumentsEachWords = new Every(countDocumentsEachWords, new Fields("word"), new Count(new Fields("countDocumentsEachWords")), Fields.ALL) ;


		Pipe join = new CoGroup(wordcount, new Fields("word"), countDocumentsEachWords, new Fields("word"), new Fields("id","word","counts","word1","countDocumentsEachWords"));
        join = new Discard(join, new Fields("word1"));

		Pipe maxValueInDoc = new Pipe("maxValueInDoc",join);
		maxValueInDoc = new GroupBy(maxValueInDoc, new Fields("id"), Fields.ALL);
		maxValueInDoc = new Every(maxValueInDoc, new Fields("counts", Integer.TYPE), new MaxValue(new Fields("MaxValueInDoc")), Fields.ALL);

		Pipe Join2 = new CoGroup(join, new Fields("id"), maxValueInDoc, new Fields("id"), new Fields("id","word","counts","countDocumentsEachWords","id1","MaxValueInDoc"));
		Join2 = new Discard(Join2,new Fields("id1"));

		Pipe  tfPipe = new Pipe("tf", Join2);

		//tfPipe = new Each(tfPipe, new Fields("MaxValueInDoc"),new ExpressionFunction(new Fields("/"),"MaxValueInDoc"), Fields.ALL);
		tfPipe = new Each(tfPipe, new Fields("MaxValueInDoc","counts"),new CustomFunction(new Fields("TF")), Fields.ALL);

		Pipe idfPipe = new Each(tfPipe, new Fields("countDocumentsEachWords","TF"), new CustomFunction1(new Fields("tfidf")), Fields.ALL );



		Pipe last = new Pipe("last",idfPipe);
		return FlowDef.flowDef()
				.addSource(last, source)
				.addTail(last)
				.addSink(last,sink);

	}
	
}
