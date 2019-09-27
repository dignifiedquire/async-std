use crate::io;

use std::process::Stdio;

#[derive(Debug)]
pub struct Child {}

pub fn spawn(
    cmd: &mut std::process::Command,
    default: Stdio,
    needs_stdin: bool,
) -> io::Result<Child> {
    unimplemented!()
}
