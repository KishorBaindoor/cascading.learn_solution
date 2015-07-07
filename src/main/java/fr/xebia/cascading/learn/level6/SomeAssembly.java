package fr.xebia.cascading.learn.level6;

import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Created by kishorb on 7/7/2015.
 */
public class SomeAssembly extends SubAssembly {
    public SomeAssembly(Pipe parties, Pipe presidents)
    {
       setPrevious(parties, presidents);
        Pipe join = new Pipe("Join");

        join = new HashJoin(parties, new Fields("year"), presidents, new Fields("year"), new Fields("year","parties","year1","presidents"));
        Pipe split1 = new Each(join, new Fields("year"),new ExpressionFilter("year.equals(\"1958\")", String.class));
        Pipe split2 = new Each(join, new Fields("year"),new ExpressionFilter("year.equals(\"1958\")", String.class));
        setTails(split1,split2);

    }
}
