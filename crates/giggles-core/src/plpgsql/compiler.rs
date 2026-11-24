use super::lexer::{Token, eof, error, lex, syntax_token};
use super::sql::{Binding, command, template};
use super::*;
use std::collections::HashMap;

type Result<T> = std::result::Result<T, PlCompileError>;

pub(super) fn compile(body: &str, sig: &FunctionSignature) -> Result<CompiledFunction> {
    if sig.kind == FunctionKind::Trigger && !sig.arguments.is_empty() {
        let mut err = error(
            "42P13",
            "trigger functions cannot have declared arguments",
            None,
        );
        err.hint = Some(
            "The arguments of the trigger can be accessed through TG_NARGS and TG_ARGV instead."
                .into(),
        );
        return Err(err);
    }
    let mut bindings = HashMap::from([("found".into(), Binding::Scalar)]);
    if sig.kind == FunctionKind::Trigger {
        for name in [
            "tg_name",
            "tg_when",
            "tg_level",
            "tg_op",
            "tg_relid",
            "tg_relname",
            "tg_table_name",
            "tg_table_schema",
            "tg_nargs",
        ] {
            bindings.insert(name.into(), Binding::Scalar);
        }
        bindings.insert("tg_argv".into(), Binding::TriggerArguments);
        bindings.insert("new".into(), Binding::Record);
        bindings.insert("old".into(), Binding::Record);
    }
    for arg in &sig.arguments {
        if let Some(name) = &arg.name {
            bindings.insert(name.clone(), Binding::Scalar);
        }
    }
    let mut compiler = Compiler {
        body,
        tokens: lex(body)?,
        at: 0,
        newlines: body.match_indices('\n').map(|(at, _)| at).collect(),
        bindings: vec![bindings],
        control: Vec::new(),
    };
    let label = compiler.label()?;
    let block = compiler.block(label, 0)?;
    compiler.eat(";");
    if compiler.peek().is_some() {
        return Err(compiler.syntax());
    }
    Ok(CompiledFunction {
        signature: Arc::new(sig.clone()),
        block: Arc::new(block),
    })
}

struct Compiler<'a> {
    body: &'a str,
    tokens: Vec<Token<'a>>,
    at: usize,
    newlines: Vec<usize>,
    bindings: Vec<HashMap<String, Binding>>,
    control: Vec<(Option<String>, bool)>,
}

fn matches_token(token: &Token<'_>, expected: &str) -> bool {
    if expected.as_bytes()[0].is_ascii_alphabetic() {
        token.word(expected)
    } else {
        token.raw == expected
    }
}

impl Compiler<'_> {
    fn peek(&self) -> Option<&Token<'_>> {
        self.tokens.get(self.at)
    }

    fn is(&self, expected: &str) -> bool {
        self.peek()
            .is_some_and(|token| matches_token(token, expected))
    }

    fn eat(&mut self, expected: &str) -> bool {
        let found = self.is(expected);
        self.at += usize::from(found);
        found
    }

    fn expect(&mut self, expected: &str) -> Result<()> {
        if self.eat(expected) {
            Ok(())
        } else {
            Err(self.syntax())
        }
    }

    fn syntax(&self) -> PlCompileError {
        self.peek()
            .map_or_else(|| eof(self.body.len()), syntax_token)
    }

    fn line(&self) -> usize {
        let position = self.peek().map_or(self.body.len(), |token| token.start);
        self.newlines.partition_point(|offset| *offset < position) + 1
    }

    fn name(&mut self) -> Result<String> {
        let name = self
            .peek()
            .and_then(Token::identifier)
            .ok_or_else(|| self.syntax())?;
        if name.is_empty() {
            return Err(self.syntax());
        }
        let name = name.to_owned();
        self.at += 1;
        Ok(name)
    }

    fn label(&mut self) -> Result<Option<String>> {
        if !self.eat("<<") {
            return Ok(None);
        }
        let name = self.name()?;
        self.expect(">>")?;
        Ok(Some(name))
    }

    fn end_label(&mut self, label: Option<&str>) -> Result<()> {
        if let Some(token) = self.peek()
            && let Some(end) = token.identifier()
        {
            if Some(end) != label {
                let message = match label {
                    Some(start) => {
                        format!("end label \"{end}\" differs from block's label \"{start}\"")
                    }
                    None => format!("end label \"{end}\" specified for unlabeled block"),
                };
                return Err(error("42601", message, Some(token.start)));
            }
            self.at += 1;
        }
        Ok(())
    }

    fn lookup(&self, name: &str) -> Option<Binding> {
        self.bindings
            .iter()
            .rev()
            .find_map(|scope| scope.get(name).copied())
    }

    fn target(&mut self) -> Result<Target> {
        let start = self.peek().map(|token| token.start);
        let name = self.name()?;
        let field = if self.eat(".") {
            Some(self.name()?)
        } else {
            None
        };
        if self.lookup(&name).is_none_or(|binding| {
            (field.is_some() && binding != Binding::Record) || binding == Binding::TriggerArguments
        }) {
            let qualified = match &field {
                Some(field) => format!("{name}.{field}"),
                None => name.clone(),
            };
            return Err(error(
                "42601",
                format!("\"{qualified}\" is not a known variable"),
                start,
            ));
        }
        Ok(Target { name, field })
    }

    fn block(&mut self, label: Option<String>, depth: usize) -> Result<Block> {
        self.bindings.push(HashMap::new());
        self.control.push((label.clone(), false));
        let mut declarations = Vec::new();
        if self.eat("declare") {
            while !self.is("begin") {
                declarations.push(self.declaration()?);
            }
        }
        self.expect("begin")?;
        let statements = self.statements(&["end"], depth)?;
        self.expect("end")?;
        // At the top level the trailing semicolon may be omitted.
        self.end_label(label.as_deref())?;
        self.control.pop();
        self.bindings.pop();
        Ok(Block {
            label,
            declarations,
            statements,
        })
    }

    fn declaration(&mut self) -> Result<Declaration> {
        let line = self.line();
        let start = self.peek().map(|token| token.start);
        let name = self.name()?;
        if self.bindings.last().unwrap().contains_key(&name) {
            return Err(error(
                "42601",
                format!("duplicate declaration at or near \"{name}\""),
                start,
            ));
        }
        let constant = self.eat("constant");
        let first = self.at;
        let end = self.scan(&["not", "default", ":=", "=", ";"])?;
        let tokens = &self.tokens[first..end];
        let spelling = self.fragment(first, end);
        let ty = if tokens.len() == 1 && tokens[0].word("record") {
            None
        } else {
            let data_type = crate::relational::plpgsql_data_type(spelling).map_err(|err| {
                use crate::relational::SqlError;
                if err == SqlError::Unsupported {
                    error(
                        "42704",
                        format!("type \"{spelling}\" does not exist"),
                        Some(tokens[0].start),
                    )
                } else {
                    syntax_token(&tokens[0])
                }
            })?;
            Some(ScalarType {
                data_type,
                spelling: spelling.into(),
                has_modifier: tokens.iter().any(|t| t.raw == "("),
            })
        };
        self.at = end;
        let not_null = self.eat("not");
        if not_null {
            self.expect("null")?;
        }
        let default = if self.eat("default") || self.eat(":=") || self.eat("=") {
            Some(self.value_expr()?)
        } else {
            None
        };
        if (not_null || constant) && default.is_none() {
            return Err(error(
                "42601",
                format!("variable \"{name}\" must have a default value"),
                start,
            ));
        }
        self.expect(";")?;
        let binding = if ty.is_some() {
            Binding::Scalar
        } else {
            Binding::Record
        };
        self.bindings
            .last_mut()
            .unwrap()
            .insert(name.clone(), binding);
        Ok(Declaration {
            line,
            name,
            ty,
            constant,
            not_null,
            default,
        })
    }

    fn statements(&mut self, until: &[&str], depth: usize) -> Result<Vec<Statement>> {
        if depth > 128 {
            return Err(error(
                "54001",
                "PL/pgSQL block nesting is too deep",
                self.peek().map(|t| t.start),
            ));
        }
        let mut statements = Vec::new();
        while self.peek().is_some() && !until.iter().any(|word| self.is(word)) {
            let line = self.line();
            let kind = self.statement(depth)?;
            self.expect(";")?;
            statements.push(Statement { line, kind });
        }
        Ok(statements)
    }

    fn statement(&mut self, depth: usize) -> Result<Stmt> {
        let start = self.at;
        let label = self.label()?;
        if self.is("begin") || self.is("declare") {
            return Ok(Stmt::Block(self.block(label, depth + 1)?));
        }
        if self.is("loop") || self.is("while") || self.is("for") {
            return self.loop_stmt(label, depth + 1);
        }
        if label.is_some() {
            return Err(self.syntax());
        }
        if self.eat("if") {
            return self.if_stmt(depth + 1);
        }
        if self.eat("case") {
            return self.case_stmt(depth + 1);
        }
        if self.is("exit") || self.is("continue") {
            return self.loop_control();
        }
        if self.eat("return") {
            return self.return_stmt();
        }
        if self.eat("null") {
            return Ok(Stmt::Null);
        }
        if self.eat("perform") {
            return Ok(Stmt::Perform(self.expr(&[";"])?));
        }
        if self.eat("raise") {
            return self.raise();
        }
        if self.eat("get") {
            self.expect("diagnostics")?;
            let target = self.target()?;
            if !self.eat(":=") {
                self.expect("=")?;
            }
            self.expect("row_count")?;
            return Ok(Stmt::Diagnostics(target));
        }
        if ["insert", "update", "delete", "select", "with"]
            .iter()
            .any(|word| self.is(word))
        {
            return self.sql_statement();
        }
        let target = self.target()?;
        if !self.eat(":=") {
            self.expect("=")?;
        }
        let expr = self.value_expr()?;
        Ok(Stmt::Assign(
            target,
            expr,
            self.fragment(start, self.at).into(),
        ))
    }

    fn if_stmt(&mut self, depth: usize) -> Result<Stmt> {
        let mut branches = Vec::new();
        loop {
            let condition = self.expr(&["then"])?;
            self.expect("then")?;
            branches.push((
                condition,
                self.statements(&["elsif", "else", "end"], depth)?,
            ));
            if !self.eat("elsif") {
                break;
            }
        }
        let otherwise = if self.eat("else") {
            self.statements(&["end"], depth)?
        } else {
            Vec::new()
        };
        self.expect("end")?;
        self.expect("if")?;
        Ok(Stmt::If(branches, otherwise))
    }

    fn case_stmt(&mut self, depth: usize) -> Result<Stmt> {
        let value = if self.is("when") {
            None
        } else {
            Some(self.expr(&["when"])?)
        };
        let mut arms = Vec::new();
        self.expect("when")?;
        loop {
            let mut values = vec![self.expr(&[",", "then"])?];
            if value.is_some() {
                while self.eat(",") {
                    values.push(self.expr(&[",", "then"])?);
                }
            }
            if value.is_some() {
                for condition in &mut values {
                    let expr = condition
                        .text
                        .strip_prefix("SELECT ")
                        .expect("CASE template");
                    condition.text =
                        format!("SELECT (${} = ({expr}))", condition.params.len() + 1).into();
                }
            }
            self.expect("then")?;
            arms.push((values, self.statements(&["when", "else", "end"], depth)?));
            if !self.eat("when") {
                break;
            }
        }
        let otherwise = if self.eat("else") {
            Some(self.statements(&["end"], depth)?)
        } else {
            None
        };
        self.expect("end")?;
        self.expect("case")?;
        Ok(Stmt::Case {
            value,
            arms,
            otherwise,
        })
    }

    fn loop_stmt(&mut self, label: Option<String>, depth: usize) -> Result<Stmt> {
        let kind = if self.eat("for") {
            let name = self.name()?;
            self.expect("in")?;
            let reverse = self.eat("reverse");
            let lower = self.expr(&["..", "loop"])?;
            if !self.eat("..") {
                return Err(error(
                    "0A000",
                    "query FOR loops are not supported",
                    self.peek().map(|t| t.start),
                ));
            }
            let upper = self.expr(&["by", "loop"])?;
            let step = if self.eat("by") {
                Some(self.expr(&["loop"])?)
            } else {
                None
            };
            self.bindings
                .push(HashMap::from([(name.clone(), Binding::Scalar)]));
            LoopKind::Integer(Box::new(IntegerFor {
                name,
                reverse,
                lower,
                upper,
                step,
            }))
        } else if self.eat("while") {
            LoopKind::While(self.expr(&["loop"])?)
        } else {
            LoopKind::Plain
        };
        self.expect("loop")?;
        self.control.push((label.clone(), true));
        let statements = self.statements(&["end"], depth)?;
        self.control.pop();
        self.expect("end")?;
        self.expect("loop")?;
        self.end_label(label.as_deref())?;
        if matches!(kind, LoopKind::Integer(_)) {
            self.bindings.pop();
        }
        Ok(Stmt::Loop {
            label,
            kind,
            statements,
        })
    }

    fn loop_control(&mut self) -> Result<Stmt> {
        let position = self.peek().map(|t| t.start);
        let exit = self.eat("exit");
        if !exit {
            self.expect("continue")?;
        }
        let label = if self.is("when") || self.is(";") || self.peek().is_none() {
            None
        } else {
            Some(self.name()?)
        };
        let name = if exit { "EXIT" } else { "CONTINUE" };
        if let Some(label) = &label {
            match self
                .control
                .iter()
                .rev()
                .find(|(scope, _)| scope.as_ref() == Some(label))
            {
                None => {
                    return Err(error(
                        "42601",
                        format!(
                            "there is no label \"{label}\" attached to any block or loop enclosing this statement"
                        ),
                        position,
                    ));
                }
                Some((_, false)) if !exit => {
                    return Err(error(
                        "42601",
                        format!("block label \"{label}\" cannot be used in CONTINUE"),
                        position,
                    ));
                }
                _ => {}
            }
        } else if !self.control.iter().any(|(_, is_loop)| *is_loop) {
            return Err(error(
                "42601",
                format!("{name} cannot be used outside a loop, unless it has a label"),
                position,
            ));
        }
        let condition = if self.eat("when") {
            Some(self.expr(&[";"])?)
        } else {
            None
        };
        Ok(Stmt::LoopControl {
            exit,
            label,
            condition,
        })
    }

    fn return_stmt(&mut self) -> Result<Stmt> {
        if self.is(";") {
            return Ok(Stmt::Return(ReturnValue::Null));
        }
        let start = self.at;
        let end = self.scan(&[";"])?;
        let tokens = &self.tokens[start..end];
        if tokens.len() == 1 && tokens[0].word("null") {
            self.at = end;
            return Ok(Stmt::Return(ReturnValue::Null));
        }
        if tokens.len() > 3 && tokens[0].word("row") && parenthesized(&tokens[1..]) {
            let mut sql = template(
                self.body,
                &tokens[2..tokens.len() - 1],
                "SELECT ",
                None,
                &|name| self.lookup(name),
            )?;
            sql.original = self.fragment(start, end).into();
            self.at = end;
            return Ok(Stmt::Return(ReturnValue::Row(sql)));
        }
        Ok(Stmt::Return(ReturnValue::Value(self.value_expr()?)))
    }

    fn value_expr(&mut self) -> Result<ValueExpr> {
        let end = self.scan(&[";"])?;
        let mut tokens = &self.tokens[self.at..end];
        while parenthesized(tokens) {
            tokens = &tokens[1..tokens.len() - 1];
        }
        if let [token] = tokens
            && let Some(name) = token.identifier()
            && self.lookup(name) == Some(Binding::Record)
        {
            self.at = end;
            return Ok(ValueExpr::Record(name.into()));
        }
        self.expr(&[";"]).map(ValueExpr::Scalar)
    }

    fn raise(&mut self) -> Result<Stmt> {
        let level = [
            ("debug", RaiseLevel::Debug),
            ("log", RaiseLevel::Log),
            ("info", RaiseLevel::Info),
            ("notice", RaiseLevel::Notice),
            ("warning", RaiseLevel::Warning),
            ("exception", RaiseLevel::Exception),
        ]
        .into_iter()
        .find_map(|(name, level)| self.eat(name).then_some(level))
        .unwrap_or(RaiseLevel::Exception);
        let position = self.peek().map(|t| t.start);
        let mut args = Vec::new();
        let has_format = !self.is("using");
        let format = if has_format {
            let format = self
                .peek()
                .ok_or_else(|| self.syntax())?
                .string()?
                .ok_or_else(|| self.syntax())?;
            self.at += 1;
            while self.eat(",") {
                args.push(self.expr(&[",", "using", ";"])?);
            }
            let mut placeholders = 0;
            let mut chars = format.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '%' && chars.next_if_eq(&'%').is_none() {
                    placeholders += 1;
                }
            }
            if placeholders != args.len() {
                let quantity = if placeholders > args.len() {
                    "few"
                } else {
                    "many"
                };
                return Err(error(
                    "42601",
                    format!("too {quantity} parameters specified for RAISE"),
                    position,
                ));
            }
            format
        } else {
            String::new()
        };
        let mut options = Vec::new();
        if self.eat("using") {
            loop {
                let position = self.peek().map(|t| t.start);
                let option = [
                    ("errcode", RaiseOption::Code),
                    ("message", RaiseOption::Message),
                    ("detail", RaiseOption::Detail),
                    ("hint", RaiseOption::Hint),
                ]
                .into_iter()
                .find_map(|(name, option)| self.eat(name).then_some(option))
                .ok_or_else(|| self.syntax())?;
                if (option == RaiseOption::Message && has_format)
                    || options.iter().any(|(previous, _)| *previous == option)
                {
                    return Err(error("42601", "RAISE option already specified", position));
                }
                if !self.eat(":=") {
                    self.expect("=")?;
                }
                options.push((option, self.expr(&[",", ";"])?));
                if !self.eat(",") {
                    break;
                }
            }
        }
        Ok(Stmt::Raise(Box::new(Raise {
            level,
            format,
            args,
            options,
        })))
    }

    fn sql_statement(&mut self) -> Result<Stmt> {
        let first = self.at;
        let end = self.scan(&[";"])?;
        let head = first + command(&self.tokens[first..end]);
        let select = self.tokens[head].word("select");
        let modifies = ["insert", "update", "delete"]
            .iter()
            .any(|word| self.tokens[head].word(word));
        let mut nesting = Vec::new();
        let mut in_returning = false;
        let mut into = None;
        let mut omit = None;
        for i in head + 1..end {
            let t = &self.tokens[i];
            if nesting.is_empty() {
                in_returning |= t.word("returning");
                if (select || in_returning) && t.word("into") {
                    let start = t.start;
                    self.at = i + 1;
                    let strict = self.eat("strict");
                    let mut targets = vec![self.target()?];
                    while self.eat(",") {
                        targets.push(self.target()?);
                    }
                    omit = Some(start..self.tokens[self.at - 1].end());
                    into = Some(SelectInto { strict, targets });
                    break;
                }
            }
            nest(&mut nesting, t)?;
        }
        let sql = template(self.body, &self.tokens[first..end], "", omit, &|name| {
            self.lookup(name)
        })?;
        self.at = end;
        Ok(Stmt::Sql {
            sql,
            into,
            modifies,
        })
    }

    fn expr(&mut self, until: &[&str]) -> Result<Sql> {
        let end = self.scan(until)?;
        let sql = template(
            self.body,
            &self.tokens[self.at..end],
            "SELECT ",
            None,
            &|name| self.lookup(name),
        )?;
        self.at = end;
        Ok(sql)
    }

    fn scan(&self, until: &[&str]) -> Result<usize> {
        let mut nesting = Vec::new();
        for (i, token) in self.tokens.iter().enumerate().skip(self.at) {
            if nesting.is_empty() && until.iter().any(|end| matches_token(token, end)) {
                return if i == self.at {
                    Err(self.syntax())
                } else {
                    Ok(i)
                };
            }
            if token.raw == ";" {
                return Err(syntax_token(token));
            }
            nest(&mut nesting, token)?;
        }
        Err(eof(self.body.len()))
    }

    fn fragment(&self, start: usize, end: usize) -> &str {
        &self.body[self.tokens[start].start..self.tokens[end - 1].end()]
    }
}

fn nest(nesting: &mut Vec<&'static str>, token: &Token<'_>) -> Result<()> {
    match token.raw {
        "(" => nesting.push(")"),
        "[" => nesting.push("]"),
        ")" | "]" if nesting.pop() != Some(token.raw) => return Err(syntax_token(token)),
        _ if token.word("case") => nesting.push("end"),
        _ if token.word("end") && nesting.last() == Some(&"end") => {
            nesting.pop();
        }
        _ => {}
    }
    if nesting.len() > 128 {
        return Err(error(
            "54001",
            "SQL expression nesting is too deep",
            Some(token.start),
        ));
    }
    Ok(())
}

fn parenthesized(tokens: &[Token<'_>]) -> bool {
    if tokens.len() < 3 || tokens[0].raw != "(" || tokens.last().unwrap().raw != ")" {
        return false;
    }
    let mut depth = 1usize;
    for token in &tokens[1..tokens.len() - 1] {
        match token.raw {
            "(" => depth += 1,
            ")" => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}
