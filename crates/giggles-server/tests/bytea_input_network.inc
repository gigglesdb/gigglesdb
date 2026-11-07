// Authored closed bytea consumer test table; raw PG traces stay under the task J root.
const BYTEA_CONSUMER_EXPECTATIONS: &str = r####"{"cases":[{"id":"bytea-empty","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":[""],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x",""]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-empty","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":[""],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x",""]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-ascii","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["616263"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x616263","616263"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-ascii","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["616263"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x616263","616263"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-hexempty","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x",""]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-hexempty","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c78"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x",""]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-hex","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78303041626666"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x00abff","00abff"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-hex","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c78303041626666"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x00abff","00abff"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-unicode","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["c3a9f09f9880"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\xc3a9f09f9880","c3a9f09f9880"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-unicode","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["c3a9f09f9880"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\xc3a9f09f9880","c3a9f09f9880"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-null","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":[null],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[[null,null]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-null","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":[null],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[[null,null]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-odd","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c7831"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal data: odd number of digits"}},{"id":"text-odd","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c7831"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal data: odd number of digits"}},{"id":"bytea-invalid","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c787a7a"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"z\""}},{"id":"text-invalid","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c787a7a"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"z\""}},{"id":"bytea-slash","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"text-slash","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"bytea-escapedslash","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c5c"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x5c","5c"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-escapedslash","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c5c"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x5c","5c"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-octzero","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c303030"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x00","00"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-octzero","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c303030"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x00","00"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-octmax","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c333737"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\xff","ff"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-octmax","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c333737"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\xff","ff"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-octshort1","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c31"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"text-octshort1","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c31"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"bytea-octshort2","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c3132"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"text-octshort2","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c3132"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"bytea-octwide","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c343030"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"text-octwide","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c343030"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"bytea-octdigit8","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c313238"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"text-octdigit8","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c313238"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type bytea"}},{"id":"bytea-octextra","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c31323334"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x5334","5334"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"text-octextra","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["5c31323334"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x5334","5334"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"bytea-nul","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["610062"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22021","message":"invalid byte sequence for encoding \"UTF8\": 0x00"}},{"id":"text-nul","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["610062"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22021","message":"invalid byte sequence for encoding \"UTF8\": 0x00"}},{"id":"bytea-invalidutf8","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["ff"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22021","message":"invalid byte sequence for encoding \"UTF8\": 0xff"}},{"id":"text-invalidutf8","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::text::bytea::text, encode($1::text::bytea,'hex')","params_hex":["ff"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22021","message":"invalid byte sequence for encoding \"UTF8\": 0xff"}},{"id":"ws-20-between","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c783431203432"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x4142","4142"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"ws-20-inside","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78342031"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \" \""}},{"id":"ws-09-between","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c783431093432"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x4142","4142"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"ws-09-inside","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78340931"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"\t\""}},{"id":"ws-0d-between","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c7834310d3432"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x4142","4142"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"ws-0d-inside","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78340d31"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"\r\""}},{"id":"ws-0a-between","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c7834310a3432"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x4142","4142"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"ws-0a-inside","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78340a31"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"\n\""}},{"id":"ws-0b-between","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c7834310b3432"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"\u000b\""}},{"id":"ws-0b-inside","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78340b31"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"\u000b\""}},{"id":"ws-0c-between","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c7834310c3432"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"\f\""}},{"id":"ws-0c-inside","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["5c78340c31"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"\f\""}},{"id":"nested-valid","original_query":"SELECT octet_length($1::bytea), encode(substring($1::bytea FROM 1 FOR 2),'hex')","query":"SELECT octet_length($1::text::bytea), encode(substring($1::text::bytea FROM 1 FOR 2),'hex')","params_hex":["5c3130316263"],"declared":[25],"format":0,"phase":"execute","result":{"parameterTypes":[25],"resultTypes":[{"name":"octet_length","oid":23,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["3","4162"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"nested-strict-null","original_query":"SELECT overlay($1::bytea placing NULL::bytea from 1)","query":"SELECT overlay($1::bytea placing NULL::bytea from 1)","params_hex":["5c786767"],"declared":[17],"format":0,"phase":"bind","result":{"parameterTypes":[17],"resultTypes":[{"name":"overlay","oid":17,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"g\""}},{"id":"nested-text-strict-null","original_query":"SELECT overlay($1::bytea placing NULL::bytea from 1)","query":"SELECT overlay($1::text::bytea placing NULL::bytea from 1)","params_hex":["5c786767"],"declared":[25],"format":0,"phase":"bind","result":{"parameterTypes":[25],"resultTypes":[{"name":"overlay","oid":17,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"g\""}},{"id":"binary-raw","original_query":"SELECT $1::bytea::text, encode(byteasend($1::bytea),'hex')","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","params_hex":["00ff5c783431"],"declared":[17],"format":1,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x00ff5c783431","00ff5c783431"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"literal-unknown","original_query":"SELECT '\\101'::bytea::text, encode(byteasend('\\101'::bytea),'hex')","query":"SELECT '\\101'::bytea::text, encode('\\101'::bytea,'hex')","params_hex":[],"declared":[],"format":0,"phase":"execute","result":{"parameterTypes":[],"resultTypes":[{"name":"text","oid":25,"format":0},{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["\\x41","41"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"literal-text","original_query":"SELECT ('\\101'::text)::bytea::text","query":"SELECT ('\\101'::text)::bytea::text","params_hex":[],"declared":[],"format":0,"phase":"execute","result":{"parameterTypes":[],"resultTypes":[{"name":"text","oid":25,"format":0}],"status":"ok","rows":[["\\x41"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"literal-escape","original_query":"SELECT E'\\\\101'::bytea::text","query":"SELECT E'\\\\101'::bytea::text","params_hex":[],"declared":[],"format":0,"phase":"execute","result":{"parameterTypes":[],"resultTypes":[{"name":"text","oid":25,"format":0}],"status":"ok","rows":[["\\x41"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"literal-lexical","original_query":"SELECT E'\\101'::bytea::text","query":"SELECT E'\\101'::bytea::text","params_hex":[],"declared":[],"format":0,"phase":"execute","result":{"parameterTypes":[],"resultTypes":[{"name":"text","oid":25,"format":0}],"status":"ok","rows":[["\\x41"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"literal-invalid","original_query":"SELECT '\\xgg'::bytea","query":"SELECT '\\xgg'::bytea","params_hex":[],"declared":[],"format":0,"phase":"parse","result":{"parameterTypes":null,"resultTypes":null,"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"g\""}},{"id":"literal-text-invalid","original_query":"SELECT ('\\xgg'::text)::bytea","query":"SELECT ('\\xgg'::text)::bytea","params_hex":[],"declared":[],"format":0,"phase":"bind","result":{"parameterTypes":[],"resultTypes":[{"name":"bytea","oid":17,"format":0}],"status":"error","sqlstate":"22023","message":"invalid hexadecimal digit: \"g\""}},{"id":"namespace-pg-catalog","original_query":null,"query":"SELECT pg_catalog.encode($1::bytea,'hex')","params_hex":["5c313031"],"declared":[17],"format":0,"phase":"execute","result":{"parameterTypes":[17],"resultTypes":[{"name":"encode","oid":25,"format":0}],"status":"ok","rows":[["41"]],"format":"PG text exact UTF8; numeric retained as text"}},{"id":"namespace-public-missing","original_query":null,"query":"SELECT public.encode($1::bytea,'hex')","params_hex":["5c313031"],"declared":[17],"format":0,"phase":"parse","result":{"parameterTypes":null,"resultTypes":null,"status":"error","sqlstate":"42883","message":"function public.encode(bytea, unknown) does not exist"}}]}"####;

fn bytea_bind(portal: &str, values: &[Option<Vec<u8>>], format: u16) -> Vec<u8> {
    let mut b = Vec::new();
    cstr(portal, &mut b);
    cstr("bytea_stmt", &mut b);
    b.extend(1u16.to_be_bytes());
    b.extend(format.to_be_bytes());
    b.extend((values.len() as u16).to_be_bytes());
    for v in values {
        if let Some(v) = v {
            b.extend((v.len() as i32).to_be_bytes());
            b.extend(v);
        } else {
            b.extend((-1i32).to_be_bytes());
        }
    }
    b.extend(0u16.to_be_bytes());
    b
}
fn bytea_code(frames: &[(u8, Vec<u8>)]) -> Option<String> {
    for (t, b) in frames {
        if *t != b'E' {
            continue;
        }
        let mut p = 0;
        while p < b.len() && b[p] != 0 {
            let kind = b[p];
            p += 1;
            let end = p + b[p..].iter().position(|x| *x == 0).unwrap();
            if kind == b'C' {
                return Some(std::str::from_utf8(&b[p..end]).unwrap().to_owned());
            }
            p = end + 1;
        }
    }
    None
}
fn bytea_oids(frames: &[(u8, Vec<u8>)]) -> (Option<Vec<u32>>, Option<Vec<u32>>) {
    let mut params = None;
    let mut cols = None;
    for (t, b) in frames {
        if *t == b't' {
            let n = u16::from_be_bytes(b[..2].try_into().unwrap()) as usize;
            params = Some(
                (0..n)
                    .map(|i| u32::from_be_bytes(b[2 + i * 4..6 + i * 4].try_into().unwrap()))
                    .collect(),
            );
        }
        if *t == b'T' {
            let n = u16::from_be_bytes(b[..2].try_into().unwrap()) as usize;
            let mut p = 2;
            let mut v = vec![];
            for _ in 0..n {
                p += b[p..].iter().position(|x| *x == 0).unwrap() + 1;
                v.push(u32::from_be_bytes(b[p + 6..p + 10].try_into().unwrap()));
                p += 18;
            }
            cols = Some(v)
        }
    }
    (params, cols)
}
fn bytea_rows(frames: &[(u8, Vec<u8>)]) -> Vec<Vec<Option<String>>> {
    let mut rows = vec![];
    for (t, b) in frames {
        if *t != b'D' {
            continue;
        }
        let n = u16::from_be_bytes(b[..2].try_into().unwrap()) as usize;
        let mut p = 2;
        let mut row = vec![];
        for _ in 0..n {
            let len = i32::from_be_bytes(b[p..p + 4].try_into().unwrap());
            p += 4;
            if len < 0 {
                row.push(None)
            } else {
                let len = len as usize;
                row.push(Some(
                    std::str::from_utf8(&b[p..p + len]).unwrap().to_owned(),
                ));
                p += len
            }
        }
        assert_eq!(p, b.len());
        rows.push(row)
    }
    rows
}
fn bytea_frames_json(frames: &[(u8, Vec<u8>)]) -> serde_json::Value {
    serde_json::Value::Array(
        frames
            .iter()
            .map(|(t, b)| serde_json::json!({"tag":char::from(*t),"hex":hex::encode(b)}))
            .collect(),
    )
}
async fn bytea_prepare(
    raw: &mut tokio_rustls::client::TlsStream<TcpStream>,
    q: &str,
    oids: &[u32],
) -> Vec<(u8, Vec<u8>)> {
    pg_send(raw, b'P', &parse_oids("bytea_stmt", q, oids)).await;
    pg_send(raw, b'D', b"Sbytea_stmt\0").await;
    pg_sync(raw).await
}
async fn bytea_cycle(
    raw: &mut tokio_rustls::client::TlsStream<TcpStream>,
    values: &[Option<Vec<u8>>],
    format: u16,
) -> Vec<(u8, Vec<u8>)> {
    pg_send(raw, b'B', &bytea_bind("bytea_portal", values, format)).await;
    pg_send(raw, b'E', &execute("bytea_portal", 0)).await;
    pg_sync(raw).await
}
async fn bytea_close(raw: &mut tokio_rustls::client::TlsStream<TcpStream>) {
    pg_send(raw, b'C', b"Sbytea_stmt\0").await;
    let f = pg_sync(raw).await;
    assert!(bytea_code(&f).is_none(), "{f:?}");
}
#[tokio::test]
async fn bytea_consumers_actual_tls_current_profile() {
    let fixture: serde_json::Value = serde_json::from_str(BYTEA_CONSUMER_EXPECTATIONS).unwrap();
    let f = Fixture::new().await;
    let before = f.admin.snapshot().unwrap();
    let mut raw = raw_pg(&f).await;
    let mut reports = vec![];
    let mut gaps = vec![];
    for (index, c) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        let id = c["id"].as_str().unwrap();
        let q = c["query"].as_str().unwrap();
        let oids: Vec<u32> = c["declared"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as u32)
            .collect();
        let prep = bytea_prepare(&mut raw, q, &oids).await;
        let prep_error = bytea_code(&prep);
        let result = &c["result"];
        let expected_state = result["sqlstate"].as_str();
        let expected_phase = c["phase"].as_str().unwrap();
        let mut local = vec![];
        let values: Vec<Option<Vec<u8>>> = c["params_hex"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().map(|v| hex::decode(v).unwrap()))
            .collect();
        let format = c["format"].as_u64().unwrap() as u16;
        let cycle = if prep_error.is_none() {
            bytea_cycle(&mut raw, &values, format).await
        } else {
            vec![]
        };
        let cycle_error = bytea_code(&cycle);
        let actual_phase = if prep_error.is_some() {
            "parse"
        } else if cycle_error.is_some() && !cycle.iter().any(|f| f.0 == b'2') {
            "bind"
        } else {
            "execute"
        };
        let actual_error = prep_error.clone().or(cycle_error.clone());
        if expected_state != actual_error.as_deref() {
            local.push(format!(
                "state expected={expected_state:?} actual={actual_error:?}"
            ))
        }
        if expected_phase != actual_phase {
            local.push(format!(
                "phase expected={expected_phase} actual={actual_phase}"
            ))
        }
        if prep_error.is_none() {
            let (params, cols) = bytea_oids(&prep);
            let want_params: Vec<u32> = result["parameterTypes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_u64().unwrap() as u32)
                .collect();
            if params != Some(want_params) {
                local.push(format!("parameter OIDs {params:?}"))
            }
            if let Some(expected) = result["resultTypes"].as_array() {
                let want: Vec<u32> = expected
                    .iter()
                    .map(|x| x["oid"].as_u64().unwrap() as u32)
                    .collect();
                if cols != Some(want) {
                    local.push(format!("column OIDs {cols:?}"))
                }
            }
            if expected_state.is_none() {
                let got = bytea_rows(&cycle);
                let want: Vec<Vec<Option<String>>> = result["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| {
                        row.as_array()
                            .unwrap()
                            .iter()
                            .map(|v| v.as_str().map(str::to_owned))
                            .collect()
                    })
                    .collect();
                if got != want {
                    local.push(format!("rows expected={want:?} actual={got:?}"))
                }
                if !cycle.iter().any(|x| x.0 == b'2') || !cycle.iter().any(|x| x.0 == b'C') {
                    local.push("missing BindComplete/CommandComplete".into())
                }
            }
            if cycle_error.is_some() {
                if cycle.iter().any(|x| matches!(x.0, b'D' | b'C')) {
                    local.push("error emitted rows/completion".into())
                }
                pg_send(&mut raw, b'E', &execute("bytea_portal", 0)).await;
                let absent = pg_sync(&mut raw).await;
                if bytea_code(&absent).as_deref() != Some("34000") {
                    local.push(format!("error portal remains: {absent:?}"))
                }
            }
            bytea_close(&mut raw).await;
        }
        if prep_error.is_some()
            && prep
                .iter()
                .any(|x| matches!(x.0, b'1' | b'2' | b'D' | b'C'))
        {
            local.push("Parse error emitted success frames".into())
        }
        if index < 2 {
            assert!(local.is_empty(), "canary {id}: {local:?}")
        }
        if !local.is_empty() {
            gaps.push(serde_json::json!({"id":id,"differences":local}))
        }
        reports.push(serde_json::json!({"id":id,"query":q,"original_query":c["original_query"],"params_hex":c["params_hex"],"parse_describe":bytea_frames_json(&prep),"bind_execute":bytea_frames_json(&cycle),"phase":actual_phase,"sqlstate":actual_error}));
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    if let Ok(report_path) = std::env::var("DD68_BYTEA_TLS_REPORT") {
        std::fs::write(report_path,serde_json::to_vec_pretty(&serde_json::json!({"cases":reports,"gaps":gaps,"byteasend_exclusion":"Original PG byteasend call is not a Giggles function claim; freshly PG-confirmed direct encode projection used"})).unwrap()).unwrap();
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
    let expected:serde_json::Value=serde_json::from_str(r####"[{"differences":["phase expected=bind actual=execute"],"id":"text-odd"},{"differences":["phase expected=bind actual=execute"],"id":"text-invalid"},{"differences":["phase expected=bind actual=execute"],"id":"text-slash"},{"differences":["phase expected=bind actual=execute"],"id":"text-octshort1"},{"differences":["phase expected=bind actual=execute"],"id":"text-octshort2"},{"differences":["phase expected=bind actual=execute"],"id":"text-octwide"},{"differences":["phase expected=bind actual=execute"],"id":"text-octdigit8"},{"differences":["phase expected=bind actual=execute"],"id":"nested-text-strict-null"},{"differences":["state expected=None actual=Some(\"0A000\")","phase expected=execute actual=parse"],"id":"literal-escape"},{"differences":["state expected=None actual=Some(\"0A000\")","phase expected=execute actual=parse"],"id":"literal-lexical"},{"differences":["phase expected=bind actual=execute"],"id":"literal-text-invalid"}]"####).unwrap();
    assert_eq!(
        serde_json::json!(gaps),
        expected,
        "profile differs; gaps remain PG incompatibilities"
    );
    println!(
        "bytea_profile_pg_matches=51 bytea_profile_pg_gaps=11; passing profile gate does not claim gap compatibility"
    );
}
#[tokio::test]
async fn bytea_consumers_actual_tls_authority_transaction() {
    let f = Fixture::new().await;
    let before = f.admin.snapshot().unwrap();
    let mut raw = raw_pg(&f).await;
    let prep = bytea_prepare(
        &mut raw,
        "INSERT INTO app(id,data) VALUES(850001,$1)",
        &[17],
    )
    .await;
    assert!(bytea_code(&prep).is_none());
    assert!(bytea_code(&temporal_state_cycle(&mut raw, "BEGIN").await).is_none());
    assert!(
        bytea_code(
            &temporal_state_cycle(
                &mut raw,
                "INSERT INTO app(id,body) VALUES(850000,'rollback')"
            )
            .await
        )
        .is_none()
    );
    let bad = bytea_cycle(&mut raw, &[Some(b"\\xgg".to_vec())], 0).await;
    assert_eq!(bytea_code(&bad).as_deref(), Some("22023"));
    assert!(!bad.iter().any(|x| matches!(x.0, b'2' | b'D' | b'C')));
    assert!(bad.iter().any(|x| x.0 == b'Z' && x.1 == b"E"));
    let aborted = bytea_cycle(&mut raw, &[Some(b"\\xgg".to_vec())], 0).await;
    assert_eq!(bytea_code(&aborted).as_deref(), Some("25P02"));
    assert!(bytea_code(&temporal_state_cycle(&mut raw, "ROLLBACK").await).is_none());
    assert_eq!(f.admin.snapshot().unwrap(), before);
    bytea_close(&mut raw).await;
    pg_send(
        &mut raw,
        b'P',
        &parse_oids(
            "bytea_stmt",
            "SELECT body FROM other WHERE body=$1::text",
            &[25],
        ),
    )
    .await;
    pg_send(
        &mut raw,
        b'B',
        &bytea_bind("bytea_portal", &[Some(b"a\0b".to_vec())], 0),
    )
    .await;
    pg_send(&mut raw, b'E', &execute("bytea_portal", 0)).await;
    let denied = pg_sync(&mut raw).await;
    assert_eq!(bytea_code(&denied).as_deref(), Some("42501"));
    assert!(!denied.iter().any(|x| matches!(x.0, b'1' | b'2' | b'D')));
    let mut task = prepared_network::raw_task(&f).await;
    pg_send(
        &mut task,
        b'P',
        &parse_oids("bytea_stmt", "SELECT $1::bytea", &[17]),
    )
    .await;
    pg_send(
        &mut task,
        b'B',
        &bytea_bind("bytea_portal", &[Some(b"\\xgg".to_vec())], 0),
    )
    .await;
    pg_send(&mut task, b'E', &execute("bytea_portal", 0)).await;
    let task_denied = pg_sync(&mut task).await;
    assert_eq!(bytea_code(&task_denied).as_deref(), Some("42501"));
    assert!(
        !task_denied
            .iter()
            .any(|x| matches!(x.0, b'1' | b'2' | b'D'))
    );
    assert_eq!(f.admin.snapshot().unwrap(), before);
    if let Ok(report_path) = std::env::var("DD68_BYTEA_TLS_CONTROL_REPORT") {
        std::fs::write(report_path,serde_json::to_vec_pretty(&serde_json::json!({"bad":bytea_frames_json(&bad),"aborted":bytea_frames_json(&aborted),"denied":bytea_frames_json(&denied),"task_denied":bytea_frames_json(&task_denied)})).unwrap()).unwrap();
    }
    pg_send(&mut task, b'X', &[]).await;
    drop(task);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
#[tokio::test]
async fn bytea_consumers_excluded_path_controls() {
    let f = Fixture::new().await;
    let before = f.admin.snapshot().unwrap();
    let mut raw = raw_pg(&f).await;
    let mut report = vec![];
    for (id, query, oid, bytes, format, state, output) in [
        (
            "integer_nul",
            "SELECT $1::integer",
            23,
            &b"1\0"[..],
            0,
            Some("22P02"),
            None,
        ),
        (
            "text_nul",
            "SELECT $1::text",
            25,
            &b"a\0b"[..],
            0,
            Some("22021"),
            None,
        ),
        (
            "integer_binary",
            "SELECT $1::integer",
            23,
            &[0, 0, 0, 1][..],
            1,
            None,
            Some("1"),
        ),
        (
            "bytea_binary",
            "SELECT $1::bytea",
            17,
            &[0, 255, 92, 120, 52, 49][..],
            1,
            None,
            Some("\\x00ff5c783431"),
        ),
        // This is a preserved binary-Text profile gap, not a PostgreSQL match.
        (
            "text_binary_nul_preserved",
            "SELECT $1::text",
            25,
            &b"a\0b"[..],
            1,
            None,
            Some("a\0b"),
        ),
    ] {
        let prep = bytea_prepare(&mut raw, query, &[oid]).await;
        assert_eq!(bytea_code(&prep), None, "{id}: {prep:?}");
        let (params, columns) = bytea_oids(&prep);
        assert_eq!(params, Some(vec![oid]), "{id}");
        assert_eq!(columns, Some(vec![oid]), "{id}");
        let frames = bytea_cycle(&mut raw, &[Some(bytes.to_vec())], format).await;
        assert_eq!(bytea_code(&frames).as_deref(), state, "{id}: {frames:?}");
        if let Some(output) = output {
            assert_eq!(
                bytea_rows(&frames),
                vec![vec![Some(output.to_owned())]],
                "{id}"
            );
            assert!(frames.iter().any(|x| x.0 == b'2'));
        } else {
            assert!(!frames.iter().any(|x| matches!(x.0, b'2' | b'D' | b'C')));
            pg_send(&mut raw, b'E', &execute("bytea_portal", 0)).await;
            assert_eq!(
                bytea_code(&pg_sync(&mut raw).await).as_deref(),
                Some("34000")
            );
        }
        report.push(serde_json::json!({"id":id,"query":query,"parameter_oid":oid,"format":format,"parse_describe":bytea_frames_json(&prep),"bind_execute":bytea_frames_json(&frames),"pg_match_claim":if id=="text_binary_nul_preserved" {"excluded preserved profile gap"}else{"excluded-path preservation control"}}));
        bytea_close(&mut raw).await;
    }
    let declared_text = bytea_prepare(
        &mut raw,
        "SELECT $1::bytea::text, encode($1::bytea,'hex')",
        &[25],
    )
    .await;
    assert_eq!(bytea_code(&declared_text), None, "{declared_text:?}");
    assert!(declared_text.iter().any(|x| x.0 == b'1'));
    assert_eq!(
        bytea_oids(&declared_text),
        (Some(vec![25]), Some(vec![25, 25]))
    );
    let mut cycles = vec![];
    for (input, text, hex) in [
        ("abc", "\\x616263", "616263"),
        ("\\x0102", "\\x0102", "0102"),
    ] {
        let frames = bytea_cycle(&mut raw, &[Some(input.as_bytes().to_vec())], 0).await;
        assert_eq!(bytea_code(&frames), None, "{frames:?}");
        assert!(frames.iter().any(|x| x.0 == b'2'));
        assert_eq!(
            bytea_rows(&frames),
            vec![vec![Some(text.to_owned()), Some(hex.to_owned())]]
        );
        cycles.push(bytea_frames_json(&frames));
    }
    report.push(serde_json::json!({"id":"declared25_text_to_bytea_cast","query":"SELECT $1::bytea::text, encode($1::bytea,'hex')","parameter_oid":25,"pg_state":null,"pg_phase":"execute","profile_state":null,"profile_phase":"execute","pg_match_claim":"matches PG18 (DD-0068 F4)","parse_describe":bytea_frames_json(&declared_text),"bind_execute":cycles}));
    bytea_close(&mut raw).await;
    if let Ok(path) = std::env::var("DD68_BYTEA_EXCLUDED_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
