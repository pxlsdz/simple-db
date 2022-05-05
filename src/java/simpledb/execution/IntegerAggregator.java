package simpledb.execution;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private Field FIELD_DEFAULT = new StringField("default", 10);
    private static final long serialVersionUID = 1L;
    private class AggregatorInfo{
        int sum = 0;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        int cnt = 0;
    }
    Map<Field, AggregatorInfo> aggregatorInfoMap;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private TupleDesc tupdesc;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.aggregatorInfoMap = new HashMap<>();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        FIELD_DEFAULT = new IntField(-1);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(this.tupdesc == null){
            buildTupleDesc(tup.getTupleDesc());
        }
        IntField agField = (IntField) tup.getField(this.afield);
        if(this.gbfield == NO_GROUPING){
            doAggregator(FIELD_DEFAULT, agField.getValue());
        }else{
            doAggregator(tup.getField(this.gbfield), agField.getValue());
        }
    }
    private void doAggregator(final Field key, int val){
        AggregatorInfo aggregatorInfo = aggregatorInfoMap.getOrDefault(key, new AggregatorInfo());
        switch (this.op){
            case MIN:
                aggregatorInfo.min = Math.min(aggregatorInfo.min, val);
                break;
            case MAX:
                aggregatorInfo.max = Math.max(aggregatorInfo.max, val);
                break;
            case AVG:
                aggregatorInfo.sum += val;
                aggregatorInfo.cnt += 1;
                break;
            case SUM:
                aggregatorInfo.sum += val;
                break;
            case COUNT:
                aggregatorInfo.cnt += 1;
                break;
            default:
                throw new IllegalArgumentException("");

        }
        aggregatorInfoMap.put(key, aggregatorInfo);
    }
    private void buildTupleDesc(final TupleDesc tupleDesc){
        if(this.gbfield == NO_GROUPING){
            this.tupdesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{""});
        }else{
            this.tupdesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{tupleDesc.getFieldName(this.gbfield), tupleDesc.getFieldName(this.gbfield)});
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        if(this.gbfield == NO_GROUPING){
            Tuple tuple = new Tuple(this.tupdesc);
            tuple.setField(0, new IntField(getRealTuple(aggregatorInfoMap.get(FIELD_DEFAULT))));
            tuples.add(tuple);
        }else{
            for(Field f : this.aggregatorInfoMap.keySet()){
                Tuple tuple = new Tuple(this.tupdesc);
                tuple.setField(0, f);
                tuple.setField(1, new IntField(getRealTuple(aggregatorInfoMap.get(f))));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(this.tupdesc, tuples);
//        throw new UnsupportedOperationException("please implement me for lab2");
    }
    private int getRealTuple(AggregatorInfo aggregatorInfo){
        switch (this.op){
            case MIN:
                return aggregatorInfo.min;
            case MAX:
                return aggregatorInfo.max;
            case AVG:
                return aggregatorInfo.sum / aggregatorInfo.cnt;
            case SUM:
                return aggregatorInfo.sum;
            case COUNT:
                return aggregatorInfo.cnt;
            default:
                throw new IllegalArgumentException("");
        }
    }

}
