package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class RecordsImpl implements Records {

    private final Database db;

    public RecordsImpl() {
        db = FDBHelper.initialization();
    }

    @Override
    public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
        Transaction tx = FDBHelper.openTransaction(db);
        if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
            FDBHelper.abortTransaction(tx);
            return StatusCode.TABLE_NOT_FOUND;
        }

        if (checkKVInvalid(primaryKeys, primaryKeysValues)) {
            FDBHelper.abortTransaction(tx);
            return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
        }

        if (checkKVInvalid(attrNames, attrValues)) {
            FDBHelper.abortTransaction(tx);
            return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
        }

        TableManagerImpl tableManager = new TableManagerImpl();
        HashMap<String, AttributeType> currentAttributes = tableManager.listTables().get(tableName).getAttributes();
        HashMap<String, Object> newAttributes = new HashMap<>();
        for (int i = 0; i < attrNames.length; i++) {
            if (!currentAttributes.containsKey(attrNames[i])) {
                newAttributes.put(attrNames[i], attrValues[i]);
            }
        }

        for (String newAttribute : newAttributes.keySet()) {
            tableManager.addAttribute(tableName, newAttribute, getAttributeType(newAttributes.get(newAttribute)));
        }
        HashMap<String, AttributeType> revisedAttributes = tableManager.listTables().get(tableName).getAttributes();

        for (int i = 0; i < primaryKeys.length; i++) {
            Object primaryKeysValue = primaryKeysValues[i];
            if (getAttributeType(primaryKeysValue) != revisedAttributes.get(primaryKeys[i])) {
                FDBHelper.abortTransaction(tx);
                return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
            }
        }

        for (int i = 0; i < attrNames.length; i++) {
            Object attribute = attrValues[i];
            if (getAttributeType(attribute) != revisedAttributes.get(attrNames[i])) {
                FDBHelper.abortTransaction(tx);
                return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
            }
        }

        TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
        List<String> tableRecordsStorePath = transformer.getTableRecordsStorePath();
        DirectorySubspace dir = FDBHelper.openSubspace(tx, tableRecordsStorePath);

        for (int i = 0; i < attrNames.length; i++) {
            Tuple keyTuple = new Tuple();
            for (Object primaryKey : primaryKeysValues) {
                keyTuple = keyTuple.addObject(primaryKey);
            }
            keyTuple = keyTuple.addObject(attrNames[i]);

            if (FDBHelper.getCertainKeyValuePairInSubdirectory(dir, tx, keyTuple, tableRecordsStorePath) != null) {
                FDBHelper.abortTransaction(tx);
                return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
            }

            Tuple valueTuple = new Tuple();
            valueTuple = valueTuple.addObject(attrValues[i]);

            FDBHelper.setFDBKVPair(dir, tx, new FDBKVPair(tableRecordsStorePath, keyTuple, valueTuple));
        }

        FDBHelper.commitTransaction(tx);
        return StatusCode.SUCCESS;
    }

    @Override
    public Cursor openCursor(String tableName, Cursor.Mode mode) {
        Transaction tx = FDBHelper.openTransaction(db);
        return new Cursor(tableName, mode, tx);
    }


    @Override
    public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
        Transaction tx = FDBHelper.openTransaction(db);
        return new Cursor(tableName, attrName, attrValue, operator, mode, isUsingIndex, tx);
    }

    @Override
    public Record getFirst(Cursor cursor) {
        List<FDBKVPair> KVPair = cursor.getFirst();
        if (KVPair != null) {
            return getRecordFromKV(KVPair, cursor.getTableName());
        }
        return null;
    }

    @Override
    public Record getLast(Cursor cursor) {
        List<FDBKVPair> KVPair = cursor.getLast();
        if (KVPair != null) {
            return getRecordFromKV(KVPair, cursor.getTableName());
        }
        return null;
    }


    @Override
    public Record getNext(Cursor cursor) {
        List<FDBKVPair> KVPair = cursor.getNext();
        if (KVPair != null) {
            return getRecordFromKV(KVPair, cursor.getTableName());
        }
        return null;
    }

    @Override
    public Record getPrevious(Cursor cursor) {
        List<FDBKVPair> KVPair = cursor.getPrevious();
        if (KVPair != null) {
            return getRecordFromKV(KVPair, cursor.getTableName());
        }
        return null;
    }

    @Override
    public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
        return cursor.update(attrNames, attrValues);
    }

    @Override
    public StatusCode deleteRecord(Cursor cursor) {
        return cursor.delete();
    }

    @Override
    public StatusCode commitCursor(Cursor cursor) {
        cursor.commit();
        return StatusCode.SUCCESS;
    }

    @Override
    public StatusCode abortCursor(Cursor cursor) {
        cursor.abort();
        return StatusCode.SUCCESS;
    }

    @Override
    public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
        return null;
    }

    private boolean checkKVInvalid(Object[] keys, Object[] values) {
        return (keys == null || keys.length == 0 || values == null || values.length == 0 || keys.length != values.length);
    }

    private AttributeType getAttributeType(Object attribute) {
        if (attribute instanceof Integer || attribute instanceof Long) {
            return AttributeType.INT;
        }

        if (attribute instanceof Double) {
            return AttributeType.DOUBLE;
        }

        if (attribute instanceof String) {
            return AttributeType.VARCHAR;
        } else {
            return AttributeType.NULL;
        }
    }

    private Record getRecordFromKV(List<FDBKVPair> KVPair, String tableName) {
        Record record = new Record();

        TableManagerImpl tableManager = new TableManagerImpl();
        List<String> primaryKeys = tableManager.listTables().get(tableName).getPrimaryKeys();
        for (int i = 0; i < primaryKeys.size(); i++) {
            record.setAttrNameAndValue(primaryKeys.get(i), KVPair.get(i).getKey().get(i));
        }

        for (FDBKVPair kvpair : KVPair) {
            record.setAttrNameAndValue(kvpair.getKey().getString(primaryKeys.size()), kvpair.getValue().get(0));
        }
        return record;
    }

}
