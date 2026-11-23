//! Runtime scope for demand-driven scalar subplans. CASE and WHERE evaluate
//! this internal scalar through their ordinary expression evaluation paths.
use super::*;

pub(in crate::relational::query) const NAME: &str = "\0plan_lazy_apply";

#[derive(Clone, Copy)]
struct Callback {
    context: *const (),
    call: unsafe fn(*const (), usize, &str) -> R<Value>,
}

thread_local! {
    static ACTIVE: Cell<Option<Callback>> = const { Cell::new(None) };
}

pub(super) fn with<F, T>(apply: &F, run: impl FnOnce() -> T) -> T
where
    F: Fn(usize, &str) -> R<Value>,
{
    unsafe fn invoke<F: Fn(usize, &str) -> R<Value>>(
        context: *const (),
        index: usize,
        name: &str,
    ) -> R<Value> {
        // SAFETY: with installs the pointer with this monomorphized callback
        // and restores the previous entry before apply can go out of scope.
        let apply = unsafe { &*context.cast::<F>() };
        apply(index, name)
    }
    struct Restore(Option<Callback>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ACTIVE.set(self.0);
        }
    }
    let _restore = Restore(ACTIVE.replace(Some(Callback {
        context: std::ptr::from_ref(apply).cast(),
        call: invoke::<F>,
    })));
    run()
}

pub(in crate::relational::query) fn evaluate(index: Value, name: Value) -> R<Value> {
    let (Value::Int(index), Value::Text(name)) = (index, name) else {
        return Err(SqlError::Type);
    };
    let active = ACTIVE.get().ok_or(SqlError::Type)?;
    // SAFETY: the thread-local callback exists only while its borrowed context
    // is live. Nested calls replace it within their own scope.
    unsafe {
        (active.call)(
            active.context,
            index.try_into().map_err(|_| SqlError::Type)?,
            &name,
        )
    }
}

pub(super) fn scalar(
    name: &str,
    ty: &DataType,
    row_slot: usize,
    metadata: Option<&BoundScalar>,
) -> R<Scalar> {
    functions::collation::scalar(
        Scalar::Call(
            NAME.into(),
            vec![
                typed(Value::Null, ty),
                join::col(row_slot),
                Scalar::Literal(Value::Text(name.into())),
            ],
        ),
        metadata,
    )
}

pub(super) fn subplans_last(filter: &mut Filter) -> R<()> {
    fn partition(filter: Filter, simple: &mut Vec<Filter>, delayed: &mut Vec<Filter>) -> R<()> {
        if let Filter::Logic(left, true, right) = filter {
            partition(*left, simple, delayed)?;
            partition(*right, simple, delayed)?;
        } else {
            let mut filter = filter;
            let mut has_apply = false;
            map_filter(&mut filter, &mut |s| {
                has_apply |= matches!(s, Scalar::Column(c) if c.name.starts_with("\0plan_apply_"));
                Ok(())
            })?;
            if has_apply {
                delayed.push(filter);
            } else {
                simple.push(filter);
            }
        }
        Ok(())
    }
    fn and(filters: &mut impl Iterator<Item = Filter>, size: usize) -> Filter {
        if size == 1 {
            return filters.next().expect("one conjunct");
        }
        let mid = size / 2;
        Filter::Logic(
            Box::new(and(filters, mid)),
            true,
            Box::new(and(filters, size - mid)),
        )
    }
    if matches!(filter, Filter::Logic(_, true, _)) {
        let mut simple = vec![];
        let mut delayed = vec![];
        partition(filter.clone(), &mut simple, &mut delayed)?;
        if !simple.is_empty() && !delayed.is_empty() {
            let count = simple.len();
            let guard = and(&mut simple.into_iter(), count);
            let guard = Filter::Is(Box::new(guard), Some(true), false);
            let count = delayed.len();
            *filter = Filter::Logic(
                Box::new(guard),
                true,
                Box::new(and(&mut delayed.into_iter(), count)),
            );
        }
    }
    Ok(())
}
