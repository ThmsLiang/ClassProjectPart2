package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class Cursor {
    public enum Mode {
        READ,
        READ_WRITE
    }
    private final Transaction tx;
    private Mode mode;
    private final String tableName;
    private final DirectorySubspace subspace;
    private final TableMetadata metadata;
    private final List<String> path;

    private AsyncIterator<KeyValue> iterator;
    private Boolean isGetFirst = null;
    private String attributeKey;
    private Object attributeVal;
    private final ComparisonOperator operator;
    private KeyValue currentKV;
    private List<FDBKVPair> currentRow;

    private boolean isUsingIndex;

    public Cursor(String tableName, Cursor.Mode mode, Transaction tx) {
        this.tx = tx;

        this.mode = mode;

        this.tableName = tableName;

        TableManager tableManager = new TableManagerImpl();
        this.metadata = tableManager.listTables().get(tableName);

        TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
        this.path = transformer.getTableRecordsStorePath();
        this.subspace = FDBHelper.openSubspace(tx, path);

        this.operator = null;
    }

    public Cursor(String tableName, String attributeKey, Object attributeVal, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex, Transaction tx) {
        this.tx = tx;

        this.mode = mode;

        this.tableName = tableName;

        TableManager tableManager = new TableManagerImpl();
        this.metadata = tableManager.listTables().get(tableName);

        TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
        this.path = transformer.getTableRecordsStorePath();
        this.subspace = FDBHelper.openSubspace(tx, path);

        this.attributeKey = attributeKey;
        this.attributeVal = attributeVal;
        this.operator = operator;
        this.isUsingIndex = isUsingIndex;
    }

    public List<FDBKVPair> getFirst() {
        isGetFirst = true;

        iterator = tx.getRange(subspace.range()).iterator();

        if (!iterator.hasNext()) {
            return null;
        }
        currentKV = iterator.next();

        currentRow = toFDBKVPair(getNextKV(new ArrayList<>()));
        return currentRow;
    }

    public List<FDBKVPair> getLast() {
        isGetFirst = false;

        iterator = tx.getRange(subspace.range(), ReadTransaction.ROW_LIMIT_UNLIMITED, true).iterator();

        if (!iterator.hasNext()) {
            return null;
        }
        currentKV = iterator.next();

        currentRow = toFDBKVPair(getNextKV(new ArrayList<>()));
        return currentRow;
    }

    public List<FDBKVPair> getNext() {
        if (!isGetFirst) return null;

        if (!iterator.hasNext()) {
            currentRow = null;
            return null;
        } else {
            currentRow = toFDBKVPair(getNextKV(new ArrayList<>()));
            return currentRow;
        }
    }

    public List<FDBKVPair> getPrevious() {
        if (isGetFirst) return null;

        if (!iterator.hasNext()) {
            currentRow = null;
            return null;
        } else {
            currentRow = toFDBKVPair(getNextKV(new ArrayList<>()));
            return currentRow;
        }
    }

    public StatusCode delete() {
        if (mode == Mode.READ || mode == null) {
            return StatusCode.CURSOR_INVALID;
        }
        if (isGetFirst == null) {
            return StatusCode.CURSOR_NOT_INITIALIZED;
        }
        if (currentRow == null) {
            return StatusCode.CURSOR_REACH_TO_EOF;
        }

        for (FDBKVPair kvpair : currentRow) {
            FDBHelper.removeKeyValuePair(tx, subspace, kvpair.getKey());
        }
        return StatusCode.SUCCESS;
    }

    public StatusCode update(String[] attributeKeys, Object[] attributeVals) {
        if (mode == Mode.READ || mode == null) {
            return StatusCode.CURSOR_INVALID;
        }
        if (isGetFirst == null) {
            return StatusCode.CURSOR_NOT_INITIALIZED;
        }
        if (currentRow == null) {
            return StatusCode.CURSOR_REACH_TO_EOF;
        }

        Set<String> attributes = new HashSet<>(Arrays.asList(attributeKeys));

        for(String attr: attributes){
            if(!metadata.getPrimaryKeys().contains(attr) && !metadata.getAttributes().containsKey(attr)){
                return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
            }
        }
        for (FDBKVPair kvpair : currentRow) {
            for (int i = 0; i < attributeKeys.length; i++) {
                if (kvpair.getKey().getString(metadata.getPrimaryKeys().size()).equals(attributeKeys[i])) {
                    Tuple newValue = new Tuple().addObject(attributeVals[i]);
                    FDBHelper.setFDBKVPair(subspace, tx, new FDBKVPair(path, kvpair.getKey(), newValue));
                    FDBHelper.removeKeyValuePair(tx, subspace, kvpair.getKey());
                }
            }
        }

        for (int i = 0; i < attributeKeys.length; i++) {
            if (metadata.getPrimaryKeys().contains(attributeKeys[i])) {
                for (FDBKVPair kvpair : currentRow) {
                    FDBHelper.removeKeyValuePair(tx, subspace, kvpair.getKey());
                    Tuple newKey = new Tuple().addObject(attributeVals[i]).addObject(kvpair.getKey().getString(metadata.getPrimaryKeys().size()));
                    Tuple newValue = new Tuple().addObject(kvpair.getValue().get(0));
                    FDBHelper.setFDBKVPair(subspace, tx, new FDBKVPair(path, newKey, newValue));
                }
            }
        }
        return StatusCode.SUCCESS;
    }

    public void abort() {
        FDBHelper.abortTransaction(tx);
    }

    public void commit() {
        FDBHelper.commitTransaction(tx);
        this.mode = null;
    }

    private Tuple getPKFromKeyValue(KeyValue keyvalue) {
        Tuple pk = new Tuple();
        for (int i = 0; i < metadata.getPrimaryKeys().size(); i++) {
            pk = pk.addObject(Tuple.fromBytes(keyvalue.getKey()).popFront().get(i));
        }
        return pk;
    }

    private List<KeyValue> getNextKV(List<KeyValue> keyvalueList) {
        keyvalueList.add(currentKV);
        Tuple currPK = getPKFromKeyValue(currentKV);
        boolean isNextPK = false;
        KeyValue kv = null;
        Object target;

        if (!iterator.hasNext()) return new ArrayList<>();
        while (iterator.hasNext() && !isNextPK) {
            currentKV = iterator.next();
            if (getPKFromKeyValue(currentKV).equals(currPK)) {
                keyvalueList.add(currentKV);
            } else isNextPK = true;
        }

        if(operator == null){
            return keyvalueList;
        }

        for(KeyValue keyValue: keyvalueList){
            Tuple attributeToCompare = Tuple.fromBytes(keyValue.getKey());
            if(attributeToCompare.getString(metadata.getPrimaryKeys().size()+1).equals(attributeKey)) {
                kv = keyValue;
                break;
            }
        }

        if(metadata.getPrimaryKeys().contains(attributeKey)){
            kv = keyvalueList.get(0);
            target = Tuple.fromBytes(kv.getKey()).get(1);
        } else if(kv != null){
            target = Tuple.fromBytes(kv.getValue()).get(0);
        } else {
            return getNextKV(new ArrayList<>());
        }

        if (compare(target)) {
            return keyvalueList;
        } else {
            return getNextKV(new ArrayList<>());
        }
    }

    private List<FDBKVPair> toFDBKVPair(List<KeyValue> keyvalueList) {
        List<FDBKVPair> result = new ArrayList<>();
        for (KeyValue keyvalue : keyvalueList) {
            Tuple key = Tuple.fromBytes(keyvalue.getKey()).popFront();
            Tuple value = Tuple.fromBytes(keyvalue.getValue());
            result.add(new FDBKVPair(path, key, value));
        }
        if (result.size() == 0) return null;
        return result;
    }

    private boolean compare(Object target) {

        if (operator == ComparisonOperator.EQUAL_TO) {
            if (target instanceof Integer) {
                return target.equals(attributeVal);
            }
            if (target instanceof Long) {
                if (attributeVal instanceof Integer) {
                    return ((Long) target).intValue() == (Integer) attributeVal;
                } else {
                    return ((Long) target).longValue() == ((Long) attributeVal).longValue();
                }
            }
            if (target instanceof Double || target instanceof String) {
                return target.equals(attributeVal);
            }

        }
        if (operator == ComparisonOperator.GREATER_THAN) {
            if (target instanceof Integer) {
                return (Integer) target > (Integer) attributeVal;
            }
            if (target instanceof Long) {
                if (attributeVal instanceof Integer) {
                    return (Long) target > (Integer) attributeVal;
                } else {
                    return (Long) target > (Long) attributeVal;
                }
            }
            if (target instanceof Double) {
                return (Double) target > (Double) attributeVal;
            }
            if (target instanceof String) {
                return ((String) target).compareTo((String) attributeVal) > 0;
            }
        }

        if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
            if (target instanceof Integer) {
                return (Integer) target >= (Integer) attributeVal;
            }
            if (target instanceof Long) {
                if (attributeVal instanceof Integer) {
                    return (Long) target >= (Integer) attributeVal;
                } else {
                    return (Long) target >= (Long) attributeVal;
                }
            }
            if (target instanceof Double) {
                return (Double) target >= (Double) attributeVal;
            }
            if (target instanceof String) {
                return ((String) target).compareTo((String) attributeVal) >= 0;
            }
        }

        if (operator == ComparisonOperator.LESS_THAN) {
            if (target instanceof Integer) {
                return (Integer) target < (Integer) attributeVal;
            }
            if (target instanceof Long) {
                if (attributeVal instanceof Integer) {
                    return (Long) target < (Integer) attributeVal;
                } else {
                    return (Long) target < (Long) attributeVal;
                }
            }
            if (target instanceof Double) {
                return (Double) target < (Double) attributeVal;
            }
            if (target instanceof String) {
                return ((String) target).compareTo((String) attributeVal) < 0;
            }
        }
        if (operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) {
            if (target instanceof Integer) {
                return (Integer) target <= (Integer) attributeVal;
            }
            if (target instanceof Long) {
                if (attributeVal instanceof Integer) {
                    return (Long) target <= (Integer) attributeVal;
                } else {
                    return (Long) target < (Long) attributeVal;
                }
            }
            if (target instanceof Double) {
                return (Double) target <= (Double) attributeVal;
            }
            if (target instanceof String) {
                return ((String) target).compareTo((String) attributeVal) <= 0;
            }
        }

        return false;
    }


    public String getTableName() {
        return tableName;
    }


}


