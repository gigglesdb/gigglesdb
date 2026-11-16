
SET SESSION binlog_transaction_compression=OFF;
CREATE TABLE type_zoo.capture_ddl (id BIGINT PRIMARY KEY) ENGINE=InnoDB;
START TRANSACTION;
INSERT INTO type_zoo.zoo SELECT 2,`i8`,`u8`,`i16`,`u16`,`i24`,`u24`,`i32`,`u32`,`i64`,`u64`,`decv`,`fv`,`dv`,`bits`,`d`,`dt`,`ts`,`tm`,`yr`,`cv`,`vv`,`txt`,`bv`,`blobv`,`ev`,`sv`,`jv`,`nullable_v`,`wide_row` FROM type_zoo.zoo WHERE id=1;
UPDATE type_zoo.zoo SET vv=CONVERT(0x7570646174656420f09f9880 USING utf8mb4),nullable_v='nonnull' WHERE id=2;
DELETE FROM type_zoo.zoo WHERE id=2;
INSERT INTO _skedaddle.watermark VALUES(99,'capture-multi-statement');
COMMIT;
SET SESSION binlog_transaction_compression=ON;
START TRANSACTION;
INSERT INTO type_zoo.zoo SELECT 2,`i8`,`u8`,`i16`,`u16`,`i24`,`u24`,`i32`,`u32`,`i64`,`u64`,`decv`,`fv`,`dv`,`bits`,`d`,`dt`,`ts`,`tm`,`yr`,`cv`,`vv`,`txt`,`bv`,`blobv`,`ev`,`sv`,`jv`,`nullable_v`,`wide_row` FROM type_zoo.zoo WHERE id=1;
DELETE FROM type_zoo.zoo WHERE id=2;
COMMIT;
FLUSH BINARY LOGS;
