//! Server-side COPY endpoints. No filesystem or process access belongs in core.
use super::*;
use crate::auth::statements::CopyDestination;
use giggles_core::relational::CopySource;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    process::{Child, Command, Stdio},
};
const CLIENT_HINT: &str =
    "Anyone can COPY to stdout or from stdin. psql's \\copy command also works for anyone.";
impl State {
    pub(super) fn prepare_copy_checked(
        &mut self,
        sql: &str,
        session: &mut Session,
    ) -> R<(giggles_core::relational::Copy, Prepared)> {
        if let Ok(copy) = giggles_core::relational::Copy::parse(sql) {
            self.copy_source_privilege(session, &copy.source)?;
        }
        let result = session.prepare_copy(sql);
        let message = giggles_core::relational::take_sql_error_message();
        let result = self.diagnostic.core(result);
        if result.is_err() && message.is_some() { self.diagnostic.wire_message = message; }
        result.inspect_err(|code| {
            if *code == "42501"
                && let Ok(Some(table)) = giggles_core::relational::Copy::target(sql)
            {
                self.diagnostic.wire_message = Some(format!(
                    "permission denied for table {}",
                    table.rsplit('\0').next().unwrap_or(&table)
                ));
            }
        })
    }
    pub(super) fn prepare_copy_output(
        &mut self,
        options: &crate::auth::statements::CopyOutput,
        session: &mut Session,
    ) -> R<Prepared> {
        let prepared = core(session.prepare(&options.select)).inspect_err(|code| {
            if *code == "42501"
                && let Some(table) = &options.table_name
            {
                self.diagnostic.wire_message = Some(format!("permission denied for table {table}"));
            }
        })?;
        if !options.force_columns.is_empty() {
            let description = core(session.describe(&prepared))?;
            let all = if let Some(sql) = &options.all_select {
                let full = core(session.prepare(sql))?;
                core(session.describe(&full))?.columns.into_iter().map(|c| c.name).collect::<Vec<_>>()
            } else { description.columns.iter().map(|c| c.name.clone()).collect() };
            for name in &options.force_columns {
                if !all.contains(name) {
                    self.diagnostic.wire_message = Some(options.table_name.as_ref().map_or_else(|| format!("column \"{name}\" does not exist"), |table| format!("column \"{name}\" of relation \"{table}\" does not exist")));
                    return Err("42703");
                }
                if !description.columns.iter().any(|c| c.name == *name) {
                    self.diagnostic.wire_message = Some(format!("FORCE_QUOTE column \"{name}\" not referenced by COPY"));
                    return Err("42P10");
                }
            }
        }
        Ok(prepared)
    }

    pub(super) fn copy_privilege(&mut self, session: &Session, program: bool, from: bool) -> R<()> {
        let required = if program {
            "pg_execute_server_program"
        } else if from {
            "pg_read_server_files"
        } else {
            "pg_write_server_files"
        };
        let mut allowed = replication_role_allowed(session)?;
        if let Role::Database { name } = session.authority_role() {
            allowed |= session.service.admin.roles().builtin_capabilities_of(name)
                .map_err(|_| "42501")?.contains(required);
        }
        if allowed {
            return Ok(());
        }
        self.diagnostic.wire_message = Some(
            if program {
                "permission denied to COPY to or from an external program"
            } else if from {
                "permission denied to COPY from a file"
            } else {
                "permission denied to COPY to a file"
            }
            .into(),
        );
        self.diagnostic.wire_detail = Some(if program {
            "Only roles with privileges of the \"pg_execute_server_program\" role may COPY to or from an external program."
        } else if from {
            "Only roles with privileges of the \"pg_read_server_files\" role may COPY from a file."
        } else {
            "Only roles with privileges of the \"pg_write_server_files\" role may COPY to a file."
        }.into());
        self.diagnostic.wire_hint = Some(CLIENT_HINT.into());
        Err("42501")
    }
    pub(super) fn copy_source_privilege(
        &mut self,
        session: &Session,
        source: &CopySource,
    ) -> R<()> {
        match source {
            CopySource::Stdin => Ok(()),
            CopySource::File(_) => self.copy_privilege(session, false, true),
            CopySource::Program(_) => self.copy_privilege(session, true, true),
        }
    }
    pub(super) fn copy_destination_privilege(
        &mut self,
        session: &Session,
        dest: &CopyDestination,
    ) -> R<()> {
        match dest {
            CopyDestination::Stdout => Ok(()),
            CopyDestination::File(_) => self.copy_privilege(session, false, false),
            CopyDestination::Program(_) => self.copy_privilege(session, true, false),
        }
    }
}
fn absolute(path: &str) -> bool {
    if path.starts_with('/') {
        return true;
    }
    #[cfg(windows)]
    {
        let b = path.as_bytes();
        path.starts_with('\\')
            || (b.len() >= 3
                && b[0].is_ascii_alphabetic()
                && b[1] == b':'
                && matches!(b[2], b'/' | b'\\'))
    }
    #[cfg(not(windows))]
    {
        false
    }
}
fn resolve(session: &Session, path: &str) -> R<std::path::PathBuf> {
    if absolute(path) {
        Ok(path.into())
    } else {
        Ok(session
            .service
            .admin
            .data_directory()
            .ok_or("XX000")?
            .join(path))
    }
}
fn errno(error: &io::Error, directory: bool) -> i32 {
    #[cfg(windows)]
    {
        if directory {
            return 21;
        }
        match error.raw_os_error() {
            Some(2 | 3) => 2,
            Some(5 | 19 | 32) => 13,
            Some(4) => 24,
            Some(80 | 183) => 17,
            Some(112) => 28,
            Some(206) => 36,
            Some(267) => 20,
            Some(109 | 232) => 32,
            Some(1117) => 5,
            _ => -1,
        }
    }
    #[cfg(not(windows))]
    {
        let _ = directory;
        error.raw_os_error().unwrap_or(5)
    }
}
fn os_error(n: i32) -> (&'static str, &'static str) {
    match n {
        2 => ("58P01", "No such file or directory"),
        1 | 13 | 30 => (
            "42501",
            if n == 30 {
                "Read-only file system"
            } else if n == 1 {
                "Operation not permitted"
            } else {
                "Permission denied"
            },
        ),
        20 => ("42809", "Not a directory"),
        21 => ("42809", "Is a directory"),
        28 => ("53100", "No space left on device"),
        23 => ("53000", "Too many open files in system"),
        24 => ("53000", "Too many open files"),
        5 => ("58030", "Input/output error"),
        36 => ("42622", "File name too long"),
        17 => ("58P02", "File exists"),
        32 => ("XX000", "Broken pipe"),
        _ => ("XX000", "Unknown error"),
    }
}
pub(super) fn read_error(d: &mut Diagnostic, error: io::Error) -> &'static str {
    let (code, message) = os_error(errno(&error, false));
    d.wire_message = Some(format!("could not read from COPY file: {message}"));
    code
}
fn file_error(
    d: &mut Diagnostic,
    path: &str,
    from: bool,
    error: io::Error,
    resolved: &std::path::Path,
    directory: bool,
) -> &'static str {
    let n = errno(&error, directory);
    let n = if n == 2 && resolved.ancestors().skip(1).any(std::path::Path::is_file) {
        20
    } else {
        n
    };
    let (code, message) = os_error(n);
    d.wire_message = Some(format!(
        "could not open file \"{path}\" for {}: {message}",
        if from { "reading" } else { "writing" }
    ));
    if matches!(n, 2 | 13) {
        d.wire_hint = Some((if from {
            "COPY FROM instructs the PostgreSQL server process to read a file. You may want a client-side facility such as psql's \\copy."
        } else {
            "COPY TO instructs the PostgreSQL server process to write a file. You may want a client-side facility such as psql's \\copy."
        }).into());
    }
    code
}
pub(super) fn input_file(session: &Session, path: &str, d: &mut Diagnostic) -> R<File> {
    let resolved = resolve(session, path)?;
    if resolved.is_dir() {
        d.wire_message = Some(format!("\"{path}\" is a directory"));
        return Err("42809");
    }
    let file = File::open(&resolved).map_err(|e| file_error(d, path, true, e, &resolved, false))?;
    if file.metadata().map_err(|_| "58030")?.is_dir() {
        d.wire_message = Some(format!("\"{path}\" is a directory"));
        return Err("42809");
    }
    Ok(file)
}
pub(super) fn spawn(session: &Session, command: &str, from: bool, d: &mut Diagnostic) -> R<Child> {
    #[cfg(unix)]
    let mut cmd = {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg(command);
        cmd
    };
    #[cfg(windows)]
    let mut cmd = {
        use std::os::windows::process::CommandExt;
        let mut cmd = Command::new("cmd.exe");
        cmd.arg("/c").raw_arg(command);
        cmd
    };
    cmd.current_dir(session.service.admin.data_directory().ok_or("XX000")?);
    cmd.stderr(Stdio::inherit());
    if from {
        cmd.stdin(Stdio::null()).stdout(Stdio::piped());
    } else {
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::from(std::io::stderr()));
    }
    cmd.spawn().map_err(|e| {
        let (code, message) = os_error(errno(&e, false));
        d.wire_message = Some(format!(
            "could not execute command \"{command}\": {message}"
        ));
        code
    })
}
pub(super) fn wait(child: &mut Child, command: &str, d: &mut Diagnostic) -> R<()> {
    let status = child.wait().map_err(|_| "58030")?;
    if status.success() {
        return Ok(());
    }
    d.wire_message = Some(format!("program \"{command}\" failed"));
    d.wire_detail = Some(match status.code() {
        Some(127) => "command not found".into(),
        Some(126) => "command not executable".into(),
        Some(n) => format!("child process exited with exit code {n}"),
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                let n = status.signal().unwrap_or(0);
                let name = match n {
                    1 => "Hangup",
                    2 => "Interrupt",
                    3 => "Quit",
                    4 => "Illegal instruction",
                    5 => "Trace/breakpoint trap",
                    6 => "Aborted",
                    7 => "Bus error",
                    8 => "Floating point exception",
                    9 => "Killed",
                    10 => "User defined signal 1",
                    11 => "Segmentation fault",
                    12 => "User defined signal 2",
                    13 => "Broken pipe",
                    14 => "Alarm clock",
                    15 => "Terminated",
                    16 => "Stack fault",
                    17 => "Child exited",
                    18 => "Continued",
                    19 => "Stopped (signal)",
                    20 => "Stopped",
                    21 => "Stopped (tty input)",
                    22 => "Stopped (tty output)",
                    23 => "Urgent I/O condition",
                    24 => "CPU time limit exceeded",
                    25 => "File size limit exceeded",
                    26 => "Virtual timer expired",
                    27 => "Profiling timer expired",
                    28 => "Window changed",
                    29 => "I/O possible",
                    30 => "Power failure",
                    31 => "Bad system call",
                    _ => "",
                };
                let name = if name.is_empty() {
                    if (34..=64).contains(&n) {
                        format!("Real-time signal {}", n - 34)
                    } else {
                        format!("Unknown signal {n}")
                    }
                } else {
                    name.into()
                };
                format!("child process was terminated by signal {n}: {name}")
            }
            #[cfg(not(unix))]
            {
                "child process failed".into()
            }
        }
    });
    Err("38000")
}
pub(super) struct Sink {
    writer: Option<Box<dyn Write>>,
    child: Option<(Child, String)>,
}
impl Drop for Sink {
    fn drop(&mut self) {
        if let Some((child, _)) = &mut self.child {
            self.writer.take();
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl Sink {
    pub(super) fn open(
        dest: &CopyDestination,
        session: &Session,
        d: &mut Diagnostic,
    ) -> R<Option<Self>> {
        let (writer, child): (Box<dyn Write>, _) = match dest {
            CopyDestination::Stdout => return Ok(None),
            CopyDestination::File(path) => {
                if !absolute(path) {
                    d.wire_message = Some("relative path not allowed for COPY to file".into());
                    return Err("42602");
                }
                let resolved = resolve(session, path)?;
                let mut options = OpenOptions::new();
                options.write(true).create(true).truncate(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o644);
                }
                let file = options
                    .open(&resolved)
                    .map_err(|e| file_error(d, path, false, e, &resolved, resolved.is_dir()))?;
                (Box::new(file), None)
            }
            CopyDestination::Program(command) => {
                let mut child = spawn(session, command, false, d)?;
                (
                    Box::new(child.stdin.take().ok_or("58030")?),
                    Some((child, command.clone())),
                )
            }
        };
        Ok(Some(Self {
            writer: Some(writer),
            child,
        }))
    }
    pub(super) fn data(&mut self, bytes: &[u8], d: &mut Diagnostic) -> R<()> {
        if let Err(error) = self.writer.as_mut().unwrap().write_all(bytes) {
            self.writer.take();
            let (code, message) = os_error(errno(&error, false));
            if let Some((mut child, command)) = self.child.take() {
                wait(&mut child, &command, d)?;
                d.wire_message = Some(format!(
                    "could not write to COPY program: {message}"
                ));
            } else {
                d.wire_message = Some(format!(
                    "could not write to COPY file: {message}"
                ));
            }
            return Err(code);
        }
        Ok(())
    }
    pub(super) fn finish(mut self, d: &mut Diagnostic) -> R<()> {
        self.writer.take();
        if let Some((mut child, command)) = self.child.take() {
            wait(&mut child, &command, d)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::os_error;

    #[test]
    fn os_error_codes() {
        for (errno, code, message) in [
            (2, "58P01", "No such file or directory"),
            (13, "42501", "Permission denied"),
            (20, "42809", "Not a directory"),
            (21, "42809", "Is a directory"),
            (32, "XX000", "Broken pipe"),
            (-1, "XX000", "Unknown error"),
        ] {
            assert_eq!(os_error(errno), (code, message), "errno {errno}");
        }
    }
}
