import { reads } from "./reads.js";
import { writes } from "./writes.js";
import { transactions } from "./transactions.js";
import { errors } from "./errors.js";
export const ops = [...reads, ...writes, ...transactions, ...errors];

import { truncates } from "./truncate.js";
ops.push(...truncates);

import { gaps } from "./gaps.js";
ops.push(...gaps);
