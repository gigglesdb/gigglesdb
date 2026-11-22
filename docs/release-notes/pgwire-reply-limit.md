# PostgreSQL wire reply size limit

The PostgreSQL wire protocol checks the reply size limit before commit.
Statements whose result rows exceed the limit fail with SQLSTATE 54000 before
anything is committed.

After a successful commit, bounded control frames may take the reply over the
limit. These include command tags, notices, ParameterStatus and ReadyForQuery.
