//! Executable adapter example and trusted subprocess crash-test helper.
use giggles_membership::{
    Identity, Membership, Result,
    durable::{Fault, Node, Store},
};
fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args
        .next()
        .ok_or("usage: membership-recover PATH [before|after]")?;
    let initial = Membership::new(
        Identity {
            group: 7,
            owner_epoch: 1,
        },
        vec![1, 2, 3],
        8,
    )?;
    let store = Store::open(path, 1, initial)?;
    match args.next().as_deref().and_then(|s| s.to_str()) {
        Some("before") => store.inject(Fault::ExitBeforeCommit)?,
        Some("after") => store.inject(Fault::ExitAfterCommit)?,
        Some(_) => return Err("unknown fault".into()),
        None => {}
    }
    let mut node = Node::new(store)?;
    let out = node.drive()?;
    println!(
        "applied={} messages={} rejected={} membership={:?}",
        node.store.applied()?,
        out.messages.len(),
        out.rejected.len(),
        node.store.membership()?
    );
    Ok(())
}
