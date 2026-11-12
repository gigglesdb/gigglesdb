use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SequenceName {
    pub schema: String,
    pub name: String,
}
impl SequenceName {
    fn sql(&self) -> String {
        format!("{}.{}", ident(&self.schema), ident(&self.name))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceScope {
    /// Owned serial/identity sequences and column DEFAULT dependencies are included.
    pub tables: Vec<Table>,
    /// Explicitly selected standalone sequences are included as well.
    pub sequences: Vec<SequenceName>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sequence {
    pub name: SequenceName,
    pub identity_column: Option<(String, String, String)>,
    pub increment: i64,
    pub minimum: i64,
    pub maximum: i64,
    pub cycle: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceState {
    pub sequence: Sequence,
    pub last_value: i64,
    pub is_called: bool,
}

pub fn discover_sequences(
    connection: &mut impl Connection,
    scope: &SequenceScope,
    timeout: Duration,
) -> Result<Vec<Sequence>, Error> {
    let tables = if scope.tables.is_empty() {
        "0".into()
    } else {
        selected(&scope.tables)
    };
    let explicit = if scope.sequences.is_empty() {
        "0".into()
    } else {
        scope
            .sequences
            .iter()
            .map(|s| format!("{}::regclass", literal(&s.sql())))
            .collect::<Vec<_>>()
            .join(",")
    };
    let sql = format!(
        "SELECT n.nspname,c.relname,s.seqincrement::text,s.seqmin::text,s.seqmax::text,s.seqcycle::text,COALESCE(tn.nspname,''),COALESCE(t.relname,''),COALESCE(a.attname,'') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_sequence s ON s.seqrelid=c.oid LEFT JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=c.oid AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') LEFT JOIN pg_class t ON t.oid=d.refobjid LEFT JOIN pg_namespace tn ON tn.oid=t.relnamespace LEFT JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid AND a.attidentity<>'' WHERE c.oid IN ({explicit}) OR d.refobjid IN ({tables}) OR EXISTS(SELECT 1 FROM pg_attrdef ad JOIN pg_depend dep ON dep.classid='pg_attrdef'::regclass AND dep.objid=ad.oid AND dep.refclassid='pg_class'::regclass AND dep.refobjid=c.oid WHERE ad.adrelid IN ({tables})) ORDER BY n.nspname,c.relname"
    );
    let sequences: Vec<_> = query(connection, &sql, timeout)?
        .iter()
        .map(|row| {
            Ok(Sequence {
                name: SequenceName {
                    schema: cell(row, 0)?.into(),
                    name: cell(row, 1)?.into(),
                },
                increment: number(cell(row, 2)?)?,
                minimum: number(cell(row, 3)?)?,
                maximum: number(cell(row, 4)?)?,
                cycle: boolean(cell(row, 5)?)?,
                identity_column: (!cell(row, 8)?.is_empty())
                    .then(|| (row[6].clone(), row[7].clone(), row[8].clone())),
            })
        })
        .collect::<Result<_, Error>>()?;
    for explicit in &scope.sequences {
        if !sequences.iter().any(|s| &s.name == explicit) {
            return Err(Error::Invalid(
                "explicit sequence is missing or is not a sequence".into(),
            ));
        }
    }
    Ok(sequences)
}

pub fn read_sequence_states(
    connection: &mut impl Connection,
    sequences: &[Sequence],
    timeout: Duration,
) -> Result<Vec<SequenceState>, Error> {
    sequences
        .iter()
        .map(|sequence| {
            let rows = query(
                connection,
                &format!(
                    "SELECT last_value::text,is_called::text FROM {}",
                    sequence.name.sql()
                ),
                timeout,
            )?;
            if rows.len() != 1 {
                return Err(Error::Invalid("sequence state must have one row".into()));
            }
            Ok(SequenceState {
                sequence: sequence.clone(),
                last_value: number(cell(&rows[0], 0)?)?,
                is_called: boolean(cell(&rows[0], 1)?)?,
            })
        })
        .collect()
}

/// Exact setval, with no nextval, arithmetic, direction or cycle adjustment.
/// Repeating it while application access is fenced is idempotent. Sequence
/// changes are not transactional in PG: replay the full plan after any failure.
pub fn sequence_sync_sql(states: &[SequenceState]) -> Result<Vec<String>, Error> {
    let mut seen = BTreeSet::new();
    states
        .iter()
        .map(|state| {
            if !seen.insert(state.sequence.name.clone())
                || state.sequence.increment == 0
                || state.last_value < state.sequence.minimum
                || state.last_value > state.sequence.maximum
            {
                return Err(Error::Invalid(
                    "invalid or duplicated sequence state".into(),
                ));
            }
            Ok(format!(
                "SELECT setval({}::regclass,{},{});",
                literal(&state.sequence.name.sql()),
                state.last_value,
                state.is_called
            ))
        })
        .collect()
}

/// Call only after capture/apply/verification of the final marker, while BOTH
/// endpoints exclude application sequence access. Existing cached application
/// sessions must be drained. Adapters must not cache sequence values themselves.
/// The returned exact source snapshot is evidence for the caller's journal.
pub fn sync_sequences(
    source: &mut impl Connection,
    destination: &mut impl Connection,
    scope: &SequenceScope,
    final_boundary: &str,
    timeout: Duration,
) -> Result<Vec<SequenceState>, Error> {
    if final_boundary.is_empty() {
        return Err(Error::Invalid(
            "sequence sync requires final boundary".into(),
        ));
    }
    let sequences = discover_sequences(source, scope, timeout)?;
    if sequences != discover_sequences(destination, scope, timeout)? {
        return Err(Error::Invalid(
            "source/destination sequence definitions differ".into(),
        ));
    }
    let states = read_sequence_states(source, &sequences, timeout)?;
    for sql in sequence_sync_sql(&states)? {
        query(destination, &sql, timeout)?;
    }
    if states != read_sequence_states(destination, &sequences, timeout)? {
        return Err(Error::Invalid(
            "destination sequence verification failed".into(),
        ));
    }
    if states != read_sequence_states(source, &sequences, timeout)? {
        return Err(Error::FenceBreached(
            "source sequence changed after final boundary".into(),
        ));
    }
    Ok(states)
}

fn number(value: &str) -> Result<i64, Error> {
    value
        .parse()
        .map_err(|_| Error::Invalid("invalid sequence number".into()))
}
