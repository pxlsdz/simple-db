package simpledb.execution;

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

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private TupleDesc tupdesc;
    Map<Field, Integer> aggregatorInfoMap;
    private Field FIELD_DEFAULT = new StringField("default", 10);
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
        this.aggregatorInfoMap = new HashMap<>();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(this.op != Op.COUNT) {
            throw new IllegalArgumentException("");
        }
        // some code goes here
        if(this.tupdesc == null){
            buildTupleDesc(tup.getTupleDesc());
        }
        Field gbField = FIELD_DEFAULT;
        if(this.gbfield != NO_GROUPING){
            gbField = tup.getField(this.gbfield);
        }
        Integer orDefault = aggregatorInfoMap.getOrDefault(gbField, 0);
        aggregatorInfoMap.put(gbField, orDefault + 1);
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
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        if(this.gbfield == NO_GROUPING){
            Tuple tuple = new Tuple(this.tupdesc);
            tuple.setField(0, new IntField(aggregatorInfoMap.get(FIELD_DEFAULT)));
            tuples.add(tuple);
        }else{
            for(Field f : this.aggregatorInfoMap.keySet()){
                Tuple tuple = new Tuple(this.tupdesc);
                tuple.setField(0, f);
                tuple.setField(1, new IntField(aggregatorInfoMap.get(f)));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(this.tupdesc, tuples);
//        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
