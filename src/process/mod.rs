//! A module for working with processes.

use crate::io::{self, Read, Write};
use crate::prelude::*;
use crate::task::{Context, Poll};

use std::ffi::OsStr;
use std::path::Path;
use std::pin::Pin;

#[doc(inline)]
pub use std::process::ExitStatus;

#[doc(inline)]
pub use std::process::Output;

#[doc(inline)]
pub use std::process::Stdio;

#[cfg(not(target_os = "windows"))]
#[path = "unix/mod.rs"]
mod imp;

#[cfg(target_os = "windows")]
#[path = "windows/mod.rs"]
mod imp;

mod kill;

#[derive(Debug)]
pub struct Child {
    child: imp::Child,

    pub stdin: Option<imp::ChildStdin>,
    pub stdout: Option<imp::ChildStdout>,
    pub stderr: Option<imp::ChildStderr>,
}

impl Child {
    pub fn id(&self) -> u32 {
        unimplemented!();
    }

    pub fn kill(&mut self) -> io::Result<()> {
        unimplemented!();
    }

    pub async fn output(self) -> io::Result<Output> {
        unimplemented!();
    }
}

impl Future for Child {
    type Output = io::Result<ExitStatus>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<<Self as Future>::Output> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct ChildStdin {}

#[derive(Debug)]
pub struct ChildStdout {}

#[derive(Debug)]
pub struct ChildStderr {}

impl Write for ChildStdin {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        unimplemented!();
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        unimplemented!();
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        unimplemented!();
    }
}

impl Read for ChildStdout {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        unimplemented!()
    }
}

impl Read for ChildStderr {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Command {
    inner: std::process::Command,
}

impl Command {
    pub fn new<S: AsRef<OsStr>>(program: S) -> Command {
        Command {
            inner: std::process::Command::new(program),
        }
    }

    pub fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Command {
        self.inner.arg(arg);
        self
    }

    pub fn args<I, S>(&mut self, args: I) -> &mut Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.inner.args(args);
        self
    }

    pub fn env<K, V>(&mut self, key: K, val: V) -> &mut Command
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.inner.env(key, val);
        self
    }

    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Command
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.inner.envs(vars);
        self
    }

    pub fn env_remove<K: AsRef<OsStr>>(&mut self, key: K) -> &mut Command {
        self.inner.env_remove(key);
        self
    }

    pub fn env_clear(&mut self) -> &mut Command {
        self.inner.env_clear();
        self
    }

    pub fn current_dir<P: AsRef<Path>>(&mut self, dir: P) -> &mut Command {
        self.inner.current_dir(dir);
        self
    }

    pub fn stdin<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Command {
        self.inner.stdin(cfg);
        self
    }

    pub fn stdout<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Command {
        self.inner.stdout(cfg);
        self
    }

    pub fn stderr<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Command {
        self.inner.stderr(cfg);
        self
    }

    pub fn spawn(&mut self) -> io::Result<Child> {
        let child = imp::spawn_child(&mut self.inner)?; //, std::process::Stdio::inherit(), true)?;
        Ok(child)
    }

    pub async fn output(&mut self) -> io::Result<Output> {
        let child = imp::spawn_child(&mut self.inner)?; //, std::process::Stdio::piped(), false)?;

        let output = child.output().await?;

        Ok(output)
    }

    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        let child = imp::spawn_child(&mut self.inner)?; //, std::process::Stdio::inherit(), true)?;

        let status = child.await?;

        Ok(status)
    }
}

impl From<std::process::Command> for Command {
    fn from(inner: std::process::Command) -> Self {
        Command { inner }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::task;

    #[test]
    fn test_simple() -> io::Result<()> {
        // TODO: windows
        task::block_on(async {
            let res = Command::new("echo").arg("hello").output().await?;

            let stdout_str = std::str::from_utf8(&res.stdout).unwrap();

            assert!(res.status.success());
            assert_eq!(stdout_str.trim().to_string(), "hello");
            assert_eq!(&res.stderr, &Vec::new());

            Ok(())
        })
    }
}
