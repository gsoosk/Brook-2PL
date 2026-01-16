# Knowledge Graph for SLW-Graph and Transactions


## Database Schema
* (db:{Table Name}, rdf:type, db:Table) 
* (db:{Table Name}, db:hasAttribute, db:{Attribute Name})



## Transactions
* **Type**: (tx:{Transaction Name}, rdf:type, app:TransactionType)
* **Operations**: 
  * (op:{TransactionName}:{Database Operation Name (e.g. Read(A))}, rdf:reads, db:{TableName})
  * (op:{TransactionName}:{Database Operation Name (e.g. Write(A))}, rdf:writes, db:{TableName}) 
    * Deletes and Inserts are also considered as write
* **Locks**:
  * (shared-lock:{transactionname}:{database lock operation name (e.g. shared lock(a))}, rdf:sharelylocks, db:{TableName}) 
  * (exclusive-lock:{TransactionName}:{Database Lock Name (e.g. Exclusive Lock(A))}, rdf:exclusivelyLocks, db:{TableName})
* **Unlocks**: (unlock:{TransactionName}:{Database Unlock Name (e.g. Unlocks(A))}, rdf:unlocks, db:{TableName})
* **S-Edges**: (op:{TransactionName}:{Database Operation Name (e.g. Read(A))} / shared-lock:{transactionname}:{database lock operation name (e.g. shared lock(a))} /  exclusive-lock:{TransactionName}:{Database Lock Name (e.g. Exclusive Lock(A))} / unlock:{TransactionName}:{Database Unlock Name (e.g. Unlocks(A))} 
        , s-edge:rightBefore,
    op:{TransactionName}:{Database Operation Name (e.g. Read(A))} / shared-lock:{transactionname}:{database lock operation name (e.g. shared lock(a))} /  exclusive-lock:{TransactionName}:{Database Lock Name (e.g. Exclusive Lock(A))} /  unlock:{TransactionName}:{Database Unlock Name (e.g. Unlocks(A))} )
* **W-Edges**: (shared-lock:{transactionname}:{database lock operation name (e.g. shared lock(a))} /  exclusive-lock:{TransactionName}:{Database Lock Name (e.g. Exclusive Lock(A))}}
  , s-edge:ConflictsWith,
   shared-lock:{transactionname}:{database lock operation name (e.g. shared lock(a))} /  exclusive-lock:{TransactionName}:{Database Lock Name (e.g. Exclusive Lock(A))} } )
