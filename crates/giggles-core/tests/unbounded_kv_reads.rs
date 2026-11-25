//! Source tripwire for reads that could bypass the sequencer's publication bound.
use proc_macro2::{Delimiter, TokenStream, TokenTree};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};
use syn::{
    Attribute, Expr, Meta, Token,
    parse::Parser,
    punctuated::Punctuated,
    spanned::Spanned,
    visit::{self, Visit},
};

type Sites = BTreeMap<(String, String, String), Vec<usize>>;

const ALLOW: &str = include_str!("unbounded_kv_reads.allow");

fn requires_test(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) => {
            let Ok(args) =
                Punctuated::<Meta, Token![,]>::parse_terminated.parse2(list.tokens.clone())
            else {
                return false;
            };
            if list.path.is_ident("all") {
                args.iter().any(requires_test)
            } else {
                list.path.is_ident("any") && args.iter().all(requires_test)
            }
        }
        Meta::NameValue(_) => false,
    }
}

fn test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("test")
            || (attr.path().is_ident("cfg")
                && attr
                    .parse_args::<Meta>()
                    .is_ok_and(|cfg| requires_test(&cfg)))
    })
}

fn raw_read(name: &str, arity: usize) -> bool {
    // Match the physical API's arities without depending on receiver or keyspace
    // names. This also conservatively catches similarly shaped non-KV calls.
    raw_read_arity(name) == Some(arity)
}

fn raw_read_arity(name: &str) -> Option<usize> {
    match name {
        "get" | "seek_ge" => Some(2),
        "first_in" => Some(3),
        "range" | "visit_first_in" => Some(4),
        _ => None,
    }
}

fn kv_owner(name: &str) -> bool {
    name.starts_with("Kv") || name == "RangeKv"
}

fn kv_trait_object(ty: &syn::Type) -> bool {
    let syn::Type::TraitObject(object) = ty else {
        return false;
    };
    object.bounds.iter().any(|bound| {
        matches!(bound, syn::TypeParamBound::Trait(trait_bound)
            if trait_bound.path.segments.last().is_some_and(|segment| kv_owner(&segment.ident.to_string())))
    })
}

fn call_path(expr: &Expr) -> Option<&syn::ExprPath> {
    match expr {
        Expr::Path(path) => Some(path),
        Expr::Paren(paren) => call_path(&paren.expr),
        Expr::Group(group) => call_path(&group.expr),
        _ => None,
    }
}

fn flagged_name(name: &str) -> bool {
    kv_owner(name)
        || raw_read_arity(name).is_some()
        || matches!(
            name,
            "latest_version"
                | "latest_committed_at_most"
                | "read_at"
                | "load_view_until"
                | "VersionedRange"
        )
}

fn max_owner(name: &str) -> bool {
    matches!(name, "u64" | "Ts")
}

struct MaxTimestamp(bool);

impl<'ast> Visit<'ast> for MaxTimestamp {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        let mut tail = path.segments.iter().rev();
        if tail.next().is_some_and(|segment| segment.ident == "MAX")
            && tail
                .next()
                .is_some_and(|segment| max_owner(&segment.ident.to_string()))
        {
            self.0 = true;
        }
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        fn contains_max(tokens: TokenStream) -> bool {
            let tokens: Vec<_> = tokens.into_iter().collect();
            tokens.windows(4).any(|window| {
                matches!(window, [TokenTree::Ident(owner), TokenTree::Punct(first),
                    TokenTree::Punct(second), TokenTree::Ident(max)]
                    if max_owner(&owner.to_string()) && first.as_char() == ':'
                        && second.as_char() == ':' && max == "MAX")
            }) || tokens.into_iter().any(
                |token| matches!(token, TokenTree::Group(group) if contains_max(group.stream())),
            )
        }
        self.0 |= contains_max(node.tokens.clone());
    }
}

struct Scan<'a> {
    path: &'a str,
    function: String,
    sites: &'a mut Sites,
    child_modules: Vec<(PathBuf, bool, PathBuf)>,
    module_dir: PathBuf,
    explicit_path_dir: PathBuf,
    test_module: bool,
}

impl Scan<'_> {
    fn record(&mut self, name: String, span: proc_macro2::Span) {
        self.sites
            .entry((self.path.into(), self.function.clone(), name))
            .or_default()
            .push(span.start().line);
    }

    fn record_call(
        &mut self,
        name: &str,
        span: proc_macro2::Span,
        owner: Option<&str>,
        is_raw_read: bool,
        args: &Punctuated<Expr, Token![,]>,
    ) {
        let reader = match (owner, name) {
            (Some("VersionedRange"), "new") => "VersionedRange::new",
            (_, "latest_committed_at_most" | "read_at" | "load_view_until") => name,
            _ => {
                if is_raw_read {
                    self.record(name.into(), span);
                }
                return;
            }
        };
        let mut max = MaxTimestamp(false);
        for arg in args {
            max.visit_expr(arg);
        }
        // A MAX bound must require its own audit even at an existing call site.
        if max.0 {
            self.record(format!("{reader}@MAX"), span);
        } else if reader == "latest_committed_at_most" {
            self.record(reader.into(), span);
        }
    }

    fn scan_macro_tokens(&mut self, tokens: TokenStream) {
        let mut tokens = tokens.into_iter().peekable();
        while let Some(token) = tokens.next() {
            match token {
                TokenTree::Ident(ident) => {
                    let name = ident.to_string();
                    if !flagged_name(&name) {
                        continue;
                    }
                    // Ordinary map/slice get calls can appear in macros with
                    // Rust pattern syntax. Exclude only a proven non-KV arity.
                    if let Some(arity) = raw_read_arity(&name)
                        && let Some(TokenTree::Group(group)) = tokens.peek()
                        && group.delimiter() == Delimiter::Parenthesis
                        && let Ok(args) =
                            Punctuated::<Expr, Token![,]>::parse_terminated.parse2(group.stream())
                        && args.len() != arity
                        && args.len() != arity + 1
                    {
                        continue;
                    }
                    self.record(format!("macro:{name}"), ident.span());
                }
                TokenTree::Group(group) => self.scan_macro_tokens(group.stream()),
                _ => (),
            }
        }
    }
}

impl<'ast> Visit<'ast> for Scan<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        let test_module = self.test_module || test_only(&node.attrs);
        let name = node.ident.to_string();
        if node.content.is_some() {
            let saved_test = std::mem::replace(&mut self.test_module, test_module);
            let dir = self.module_dir.join(&name);
            let saved_dir = std::mem::replace(&mut self.module_dir, dir.clone());
            let saved_explicit = std::mem::replace(&mut self.explicit_path_dir, dir);
            visit::visit_item_mod(self, node);
            self.explicit_path_dir = saved_explicit;
            self.module_dir = saved_dir;
            self.test_module = saved_test;
        } else {
            let explicit = node.attrs.iter().find_map(|attr| match &attr.meta {
                Meta::NameValue(item) if item.path.is_ident("path") => match &item.value {
                    Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(path),
                        ..
                    }) => Some(path.value()),
                    _ => None,
                },
                _ => None,
            });
            let flat = self.module_dir.join(format!("{name}.rs"));
            let (path, dir) = match explicit {
                Some(path) => {
                    let path = self.explicit_path_dir.join(path);
                    let dir = path.parent().unwrap().to_owned();
                    (path, dir)
                }
                None if flat.is_file() => (flat, self.module_dir.join(&name)),
                None => {
                    let dir = self.module_dir.join(&name);
                    (dir.join("mod.rs"), dir)
                }
            };
            self.child_modules.push((path, test_module, dir));
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if !test_only(&node.attrs) {
            let previous = std::mem::replace(&mut self.function, node.sig.ident.to_string());
            visit::visit_item_fn(self, node);
            self.function = previous;
        }
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if !test_only(&node.attrs) {
            let previous = std::mem::replace(&mut self.function, node.sig.ident.to_string());
            visit::visit_impl_item_fn(self, node);
            self.function = previous;
        }
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        if !test_only(&node.attrs) {
            let previous = std::mem::replace(&mut self.function, node.sig.ident.to_string());
            visit::visit_trait_item_fn(self, node);
            self.function = previous;
        }
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if !self.test_module {
            let name = node.method.to_string();
            if name == "latest_version" {
                self.record(name, node.method.span());
            } else {
                self.record_call(
                    &name,
                    node.method.span(),
                    None,
                    raw_read(&name, node.args.len()),
                    &node.args,
                );
            }
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if !self.test_module
            && let Some(target) = call_path(&node.func)
            && let Some(method) = target.path.segments.last()
        {
            let name = method.ident.to_string();
            let owner = target.path.segments.iter().rev().nth(1);
            let self_type = target.qself.as_ref().map(|qself| qself.ty.as_ref());
            let self_owner = match self_type {
                Some(syn::Type::Path(path)) => path.path.segments.last(),
                _ => None,
            };
            let owner = owner.map(|segment| segment.ident.to_string());
            let self_owner = self_owner.map(|segment| segment.ident.to_string());
            let reads_kv = owner.as_deref().is_some_and(kv_owner)
                || self_owner.as_deref().is_some_and(kv_owner)
                || self_type.is_some_and(kv_trait_object);
            self.record_call(
                &name,
                method.span(),
                owner.as_deref().or(self_owner.as_deref()),
                reads_kv && raw_read(&name, node.args.len().saturating_sub(1)),
                &node.args,
            );
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if !self.test_module
            && let Some(segment) = node.path.segments.last()
            && segment.ident == "latest_version"
        {
            self.record("latest_version".into(), segment.ident.span());
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        if !self.test_module && !test_only(&node.attrs) {
            visit::visit_item_use(self, node);
        }
    }

    fn visit_use_rename(&mut self, node: &'ast syn::UseRename) {
        let name = node.ident.to_string();
        if flagged_name(&name) {
            self.record(format!("alias:{name}"), node.ident.span());
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if self.test_module {
            return;
        }
        if let Ok(args) =
            Punctuated::<Expr, Token![,]>::parse_terminated.parse2(node.tokens.clone())
        {
            for arg in &args {
                self.visit_expr(arg);
            }
        } else {
            // Opaque macro syntax can generate a read even without a parseable call.
            self.scan_macro_tokens(node.tokens.clone());
        }
    }
}

fn scan(
    path: &str,
    code: &str,
    module_dir: PathBuf,
    explicit_path_dir: PathBuf,
    test_module: bool,
    sites: &mut Sites,
) -> Vec<(PathBuf, bool, PathBuf)> {
    let ast = syn::parse_file(code).unwrap_or_else(|err| panic!("{path}: {err}"));
    let mut scan = Scan {
        path,
        function: "<module>".into(),
        sites,
        child_modules: Vec::new(),
        module_dir,
        explicit_path_dir,
        test_module,
    };
    scan.visit_file(&ast);
    scan.child_modules
}

#[test]
fn only_audited_sites_read_physical_versions() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![(src.join("lib.rs"), false, src.clone())];
    for entry in fs::read_dir(src.join("bin")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "rs") {
            pending.push((path, false, src.join("bin")));
        }
    }
    let mut visited = std::collections::BTreeSet::new();
    let mut sites = Sites::new();
    while let Some((path, test_module, dir)) = pending.pop() {
        if !visited.insert((path.clone(), test_module)) {
            continue;
        }
        let relative = path
            .strip_prefix(&src)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let code = fs::read_to_string(&path).unwrap_or_else(|err| panic!("{relative}: {err}"));
        pending.extend(scan(
            &relative,
            &code,
            dir,
            path.parent().unwrap().into(),
            test_module,
            &mut sites,
        ));
    }
    // Scan orphaned source files too. Only files reached under a test-only
    // module are exempt, regardless of their names or directory.
    let mut dirs = vec![src.clone()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs")
                && !visited.contains(&(path.clone(), false))
                && !visited.contains(&(path.clone(), true))
            {
                let relative = path
                    .strip_prefix(&src)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                let code = fs::read_to_string(&path).unwrap();
                let dir = path.parent().unwrap().to_owned();
                scan(&relative, &code, dir.clone(), dir, false, &mut sites);
            }
        }
    }
    let differences = differences(sites, ALLOW);
    assert!(
        differences.is_empty(),
        "unaudited physical reads:\n{}",
        differences.join("\n")
    );
}

fn differences(mut sites: Sites, allow: &str) -> Vec<String> {
    let mut differences = Vec::new();
    for line in allow
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let (site, reason) = line.split_once(" | ").expect("allow-list justification");
        assert!(!reason.trim().is_empty());
        let fields = site.split_ascii_whitespace().collect::<Vec<_>>();
        let [path, function, kind, count] = fields[..] else {
            panic!("invalid site: {site}")
        };
        let count: usize = count.parse().expect("allow-list count");
        let found = sites
            .remove(&(path.into(), function.into(), kind.into()))
            .unwrap_or_default();
        if found.len() != count {
            differences.push(format!(
                "{path} {function} {kind}: expected {count}, found at {found:?}"
            ));
        }
    }
    for ((path, function, kind), lines) in sites {
        differences.push(format!(
            "{path} {function} {kind} {} at {lines:?}",
            lines.len()
        ));
    }
    differences
}

#[test]
fn scanner_flags_new_physical_reads() {
    let source = r#"
        fn offender(store: &dyn KvRead, alias: Keyspace) {
            // mvcc::latest_version(store, alias, b"ignored");
            let _ = mvcc::latest_version
                (store, Keyspace::Rows, b"k");
            assert!(store.seek_ge(alias, b"k").is_ok());
            let _ = KvRead::get(store, Keyspace::Meta, b"last_commit_ts");
            let _ = store.get(alias, b"another");
            let _ = <dyn KvRead>::get(store, alias, b"ufcs");
            let _ = <KvBackend>::range(store, alias, start, end, direction);
            let _ = items.map(mvcc::latest_version);
            let _ = mvcc::latest_committed_at_most(store, alias, b"k", Ts(u64::MAX));
            let _ = mvcc::latest_committed_at_most(store, alias, b"k", visible_ts);
            let _ = mvcc::read_at(store, alias, b"k", if ready { crate::Ts::MAX } else { ts });
            let _ = (<mvcc::VersionedRange<'_>>::new)(store, alias, start, end, Ts::MAX);
            let _ = catalog.load_view_until(Ts(u64::MAX), bound);
            let _ = read_at(store, alias, b"k", timestamp!(Ts::MAX));
            let _ = mvcc::read_at(store, alias, b"k", visible_ts);
            let _ = Vec::new(Ts::MAX);
            let _ = matches!(store.get(alias, b"k"), Ok(Some(ref value)) | Ok(None));
            macro_rules! hidden { () => { mvcc::latest_version(store, alias, b"k") }; }
            let _ = matches!(items.get(0), Some(ref value) | None);
        }
        use kv::KvRead as R;
        use mvcc::{latest_version as lv, VersionedRange as AllVersions};
        #[cfg(test)]
        fn test_only() { KvRead::get(store, Keyspace::Seq, b"counter"); }
    "#;
    let mut sites = Sites::new();
    scan(
        "synthetic.rs",
        source,
        PathBuf::new(),
        PathBuf::new(),
        false,
        &mut sites,
    );
    let violations = differences(
        sites,
        "synthetic.rs offender get 1 | Existing audited call.",
    );
    assert_eq!(
        violations,
        [
            "synthetic.rs offender get: expected 1, found at [7, 8, 9]",
            "synthetic.rs <module> alias:KvRead 1 at [24]",
            "synthetic.rs <module> alias:VersionedRange 1 at [25]",
            "synthetic.rs <module> alias:latest_version 1 at [25]",
            "synthetic.rs offender VersionedRange::new@MAX 1 at [15]",
            "synthetic.rs offender latest_committed_at_most 1 at [13]",
            "synthetic.rs offender latest_committed_at_most@MAX 1 at [12]",
            "synthetic.rs offender latest_version 2 at [4, 11]",
            "synthetic.rs offender load_view_until@MAX 1 at [16]",
            "synthetic.rs offender macro:get 1 at [20]",
            "synthetic.rs offender macro:latest_version 1 at [21]",
            "synthetic.rs offender range 1 at [10]",
            "synthetic.rs offender read_at@MAX 2 at [14, 17]",
            "synthetic.rs offender seek_ge 1 at [6]",
        ]
    );
}
