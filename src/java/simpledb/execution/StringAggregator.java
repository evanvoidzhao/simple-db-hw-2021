package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private final Op what;
    private final AggHandler aggHandler;

    private abstract class AggHandler {
        Map<Field, Integer> aggResult;

        abstract void handle(Field gbField, StringField aggField);

        public AggHandler() {
            aggResult = new HashMap<>();
        }

        public Map<Field, Integer> getAggResult() {
            return aggResult;
        }
    }

    private class CountHandler extends AggHandler {

        @Override
        void handle(Field gbField, StringField aggField) {
            // TODO Auto-generated method stub
            String value = aggField.getValue();
            if (aggResult.containsKey(gbField)) {
                aggResult.put(gbField, aggResult.get(gbField) + 1);
            } else {
                aggResult.put(gbField, 1);
            }
        }
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;

        switch (what) {
            case COUNT:
                this.aggHandler = new CountHandler();
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField af = (StringField) tup.getField(this.afield);
        Field gbf = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        aggHandler.handle(gbf, af);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td;

        if (this.gbfield == NO_GROUPING){
            int result = aggHandler.getAggResult().get(null);
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            IntField intField = new IntField(result);
            Tuple tp = new Tuple(td);
            tp.setField(0, intField);
            tuples.add(tp);
        }else{
            td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE}, new String[]{"groupVal","aggregateVal"});
            for (Field f : aggHandler.getAggResult().keySet()){
                IntField intField = new IntField(aggHandler.getAggResult().get(f));
                Tuple tp = new Tuple(td);
                if (gbfieldType == Type.INT_TYPE){
                    tp.setField(0, (IntField)f);
                }else if (gbfieldType == Type.STRING_TYPE){
                    tp.setField(0, (StringField)f);
                }
                tp.setField(1, intField);
                tuples.add(tp);
            }
        
        }
        TupleIterator ti = new TupleIterator(td, tuples);
        //System.out.printf("op:%s tuples:%s \n",what, tuples);
        return ti;
    }

}
