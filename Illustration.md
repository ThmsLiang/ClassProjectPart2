# Illustration of Project Part 2

## File Structure
    The file structure for the project is shown below:
    - main directory
        - table 1
            - attributeStore
            - recordStore
        - table 2
        ....
    Where the name of the table directory will be the name of the table. 
    `attributeStore` contains the subspace storing table schema, 
    `recordStore` contains the subspace storing all records.

## Key-Value Pair
    In this project I use `FDBKVPair` to store attributes.
    Each attribute is stored as (K, V) where K -> Primary Keys + Attribute Name and V -> Attribute Value

## Cursor
    To implement Cursor, I use `AsyncIterator` from FoundationDB to go through the subspace of recordStore.
    For getLast() and getPrevious(), AsyncIterator allows you to traverse in backward order by using constructor Iterable(Range, Int, Boolean)
    To delete current record, simply use the member variable to track to the attributes in the database and perform removeKVPair
    To update current record, I perform a delete + insert operation to achieve that.
    