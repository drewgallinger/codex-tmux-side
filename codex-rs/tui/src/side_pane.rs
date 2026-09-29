//! Private, launch-owned handoff and tmux lifecycle for shared-server side chats.

use crate::AppServerTarget;
use crate::chatwidget::UserMessage;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_protocol::ThreadForkParams;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::AbsolutePathBuf;
use color_eyre::Result;
use color_eyre::eyre::bail;
use color_eyre::eyre::ensure;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 60);
const POLL_INTERVAL: Duration = Duration::from_millis(/*millis*/ 100);
const TMUX_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 5);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(remote = "RemoteAppServerEndpoint")]
enum Endpoint {
    WebSocket {
        websocket_url: String,
        auth_token: Option<String>,
    },
    UnixSocket {
        socket_path: AbsolutePathBuf,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Target {
    LocalDaemon(#[serde(with = "Endpoint")] RemoteAppServerEndpoint),
    Remote(#[serde(with = "Endpoint")] RemoteAppServerEndpoint),
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SidePaneHandoff {
    pub(crate) parent_thread_id: ThreadId,
    pub(crate) settings: ThreadForkParams,
    pub(crate) message: Option<UserMessage>,
    target: Target,
}

impl std::fmt::Debug for SidePaneHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidePaneHandoff")
            .field("parent_thread_id", &self.parent_thread_id)
            .finish_non_exhaustive()
    }
}

impl SidePaneHandoff {
    pub(crate) fn new(
        target: &AppServerTarget,
        parent_thread_id: ThreadId,
        settings: ThreadForkParams,
        message: Option<UserMessage>,
    ) -> Result<Self> {
        let target = match target {
            AppServerTarget::Embedded => bail!("embedded sessions cannot open a side pane"),
            AppServerTarget::LocalDaemon { endpoint, .. } => Target::LocalDaemon(endpoint.clone()),
            AppServerTarget::Remote { endpoint } => Target::Remote(endpoint.clone()),
        };
        Ok(Self {
            parent_thread_id,
            settings,
            message,
            target,
        })
    }

    pub(crate) fn target(&self) -> AppServerTarget {
        match &self.target {
            Target::LocalDaemon(endpoint) => AppServerTarget::LocalDaemon {
                endpoint: endpoint.clone(),
                allow_embedded_fallback: false,
            },
            Target::Remote(endpoint) => AppServerTarget::Remote {
                endpoint: endpoint.clone(),
            },
        }
    }
}

pub(crate) fn source_pane() -> Option<String> {
    std::env::var("TMUX_PANE")
        .ok()
        .filter(|pane| valid_pane(pane))
}

fn valid_pane(pane: &str) -> bool {
    pane.strip_prefix('%')
        .is_some_and(|id| !id.is_empty() && id.bytes().all(|c| c.is_ascii_digit()))
}

fn tmux() -> Result<tokio::process::Command> {
    let executable = codex_utils_path::system_executable("tmux")
        .ok_or_else(|| color_eyre::eyre::eyre!("tmux is unavailable"))?;
    let mut command = tokio::process::Command::new(executable);
    command
        .env("PATH", codex_utils_path::system_path()?)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    Ok(command)
}

async fn checked_output(command: &mut tokio::process::Command) -> Result<Vec<u8>> {
    let output = tokio::time::timeout(TMUX_TIMEOUT, command.output()).await??;
    ensure!(
        output.status.success(),
        "tmux failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output.stdout)
}

/// The parent retains this owner until the child exits or this side is replaced.
#[derive(Debug)]
pub(crate) struct SidePaneLaunch {
    directory: TempDir,
    pane_id: String,
}

impl SidePaneLaunch {
    pub(crate) async fn launch(
        handoff: &SidePaneHandoff,
        executable: &Path,
        source_pane: &str,
        codex_home: &Path,
    ) -> Result<Self> {
        ensure!(valid_pane(source_pane), "invalid source tmux pane");
        let directory = tempfile::Builder::new().prefix("codex-side-").tempdir()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                directory.path(),
                std::fs::Permissions::from_mode(/*mode*/ 0o700),
            )?;
        }
        write_json(directory.path(), "handoff", handoff)?;
        write_json(directory.path(), "parent", &source_pane)?;
        write_json(directory.path(), "parent_pid", &std::process::id())?;
        let cwd = std::env::current_dir()
            .ok()
            .filter(|path| path.is_dir())
            .unwrap_or_else(|| directory.path().to_path_buf());
        let mut command = tmux()?;
        split_command(
            &mut command,
            source_pane,
            &cwd,
            executable,
            &directory.path().join("handoff"),
            codex_home,
        );
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| color_eyre::eyre::eyre!("tmux stdout unavailable"))?;
        let mut stdout = tokio::io::BufReader::new(stdout);
        let mut pane_id = String::new();
        tokio::time::timeout(TMUX_TIMEOUT, stdout.read_line(&mut pane_id)).await??;
        let pane_id = pane_id.trim().to_owned();
        ensure!(
            valid_pane(&pane_id) && pane_id != source_pane,
            "tmux returned an invalid side pane"
        );
        // tmux prints the id before running after-split hooks. Own the pane now,
        // so a slow or failing hook cannot strand it after the client times out.
        let launch = Self { directory, pane_id };
        let output = tokio::time::timeout(TMUX_TIMEOUT, child.wait_with_output()).await??;
        ensure!(
            output.status.success(),
            "tmux failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        Ok(launch)
    }

    pub(crate) async fn wait_forked(&self) -> Result<ThreadId> {
        self.wait_for("forked").await?;
        Ok(serde_json::from_slice(&std::fs::read(
            self.directory.path().join("forked"),
        )?)?)
    }

    pub(crate) fn forked_thread_id(&self) -> Option<ThreadId> {
        serde_json::from_slice(&std::fs::read(self.directory.path().join("forked")).ok()?).ok()
    }

    pub(crate) fn acknowledge_ownership(&self) -> Result<()> {
        write_json(self.directory.path(), "owned", &())
    }

    pub(crate) async fn wait_ready(&self) -> Result<()> {
        self.wait_for("ready").await
    }

    async fn wait_for(&self, name: &str) -> Result<()> {
        tokio::time::timeout(STARTUP_TIMEOUT, async {
            loop {
                let failed = self.directory.path().join("failed");
                if failed.exists() {
                    let error: String = serde_json::from_slice(&std::fs::read(failed)?)?;
                    bail!("{error}");
                }
                if self.directory.path().join(name).exists() {
                    return Ok(());
                }
                ensure!(self.is_alive().await?, "side pane closed during startup");
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| color_eyre::eyre::eyre!("side pane startup timed out"))?
    }

    async fn is_alive(&self) -> Result<bool> {
        pane_is_alive(&self.pane_id).await
    }

    pub(crate) async fn wait_closed(&self) -> Result<()> {
        while self.is_alive().await? {
            tokio::time::sleep(Duration::from_secs(/*secs*/ 1)).await;
        }
        Ok(())
    }

    pub(crate) async fn focus(&self) -> Result<()> {
        checked_output(tmux()?.args(["select-pane", "-t", &self.pane_id])).await?;
        Ok(())
    }

    pub(crate) async fn close(&self) -> Result<()> {
        if self.is_alive().await? {
            checked_output(tmux()?.args(["kill-pane", "-t", &self.pane_id])).await?;
        }
        Ok(())
    }
}

async fn pane_is_alive(pane: &str) -> Result<bool> {
    let mut command = tmux()?;
    command.args([
        "display-message",
        "-p",
        "-t",
        pane,
        "#{pane_id}:#{pane_dead}",
    ]);
    let output = tokio::time::timeout(TMUX_TIMEOUT, command.output()).await??;
    Ok(output.status.success()
        && String::from_utf8_lossy(&output.stdout).trim() == format!("{pane}:0"))
}

impl Drop for SidePaneLaunch {
    fn drop(&mut self) {
        // The pane id belongs to this launch; never address the containing window/session.
        if let Some(executable) = codex_utils_path::system_executable("tmux")
            && let Ok(mut child) = std::process::Command::new(executable)
                .args(["kill-pane", "-t", &self.pane_id])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
        {
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + TMUX_TIMEOUT;
                while matches!(child.try_wait(), Ok(None)) {
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        break;
                    }
                    std::thread::sleep(POLL_INTERVAL);
                }
                let _ = child.wait();
            });
        }
    }
}

fn split_command(
    command: &mut tokio::process::Command,
    source: &str,
    cwd: &Path,
    executable: &Path,
    handoff: &Path,
    codex_home: &Path,
) {
    let mut home = std::ffi::OsString::from("CODEX_HOME=");
    home.push(codex_home);
    command
        .args([
            "split-window",
            "-h",
            "-d",
            "-P",
            "-F",
            "#{pane_id}",
            "-t",
            source,
            "-c",
        ])
        .arg(cwd)
        .arg("-e")
        .arg(home)
        .arg("--")
        .arg(executable)
        .arg("--side-handoff")
        .arg(handoff);
}

/// Child-side handshake token. Only the parent owns/removes the private directory.
#[derive(Clone, Debug)]
pub(crate) struct SidePaneChild {
    directory: PathBuf,
}

impl SidePaneChild {
    pub(crate) fn read(path: &Path) -> Result<(SidePaneHandoff, Self)> {
        let directory = path
            .parent()
            .ok_or_else(|| color_eyre::eyre::eyre!("invalid side handoff path"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            ensure!(
                std::fs::metadata(directory)?.mode() & 0o077 == 0,
                "side handoff directory must be private"
            );
        }
        ensure!(
            std::fs::metadata(path)?.len() <= 64 * 1024 * 1024,
            "side handoff is too large"
        );
        let handoff: SidePaneHandoff = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            handoff.settings.thread_id == handoff.parent_thread_id.to_string(),
            "side handoff parent does not match fork settings"
        );
        ensure!(
            handoff.settings.ephemeral,
            "side handoff must request an ephemeral fork"
        );
        ensure!(
            handoff.settings.path.is_none(),
            "side handoff must fork by parent thread id"
        );
        // The child now owns the payload in memory. Do not retain credentials and
        // prepared input on disk for the lifetime of the conversation.
        std::fs::remove_file(path)?;
        Ok((
            handoff,
            Self {
                directory: directory.to_owned(),
            },
        ))
    }

    pub(crate) fn forked(&self, thread_id: ThreadId) -> Result<()> {
        write_json(&self.directory, "forked", &thread_id)
    }

    pub(crate) async fn wait_for_ownership(&self) -> Result<()> {
        tokio::time::timeout(STARTUP_TIMEOUT, async {
            while !self.directory.join("owned").exists() {
                ensure!(self.directory.exists(), "parent closed during side startup");
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Ok(())
        })
        .await
        .map_err(|_| color_eyre::eyre::eyre!("parent did not acknowledge side ownership"))?
    }

    pub(crate) fn ready(&self) -> Result<()> {
        write_json(&self.directory, "ready", &())
    }

    pub(crate) fn failed(&self, error: &str) -> Result<()> {
        write_json(&self.directory, "failed", &error)
    }

    pub(crate) async fn wait_parent_closed(&self) -> Result<()> {
        let pane: String = serde_json::from_slice(&std::fs::read(self.directory.join("parent"))?)?;
        ensure!(valid_pane(&pane), "invalid parent tmux pane");
        #[cfg(unix)]
        let pid: libc::pid_t =
            serde_json::from_slice(&std::fs::read(self.directory.join("parent_pid"))?)?;
        while self.directory.exists() && pane_is_alive(&pane).await? {
            #[cfg(unix)]
            // SAFETY: signal 0 checks existence without sending a signal or dereferencing pointers.
            if unsafe {
                libc::kill(pid, /*sig*/ 0)
            } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(/*secs*/ 1)).await;
        }
        Ok(())
    }

    pub(crate) async fn focus_parent(&self) -> Result<()> {
        let pane: String = serde_json::from_slice(&std::fs::read(self.directory.join("parent"))?)?;
        ensure!(valid_pane(&pane), "invalid parent tmux pane");
        checked_output(tmux()?.args(["select-pane", "-t", &pane])).await?;
        Ok(())
    }
}

fn write_json(directory: &Path, name: &str, value: &impl Serialize) -> Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(directory)?;
    serde_json::to_writer(file.as_file_mut(), value)?;
    file.persist(directory.join(name))?;
    Ok(())
}

#[cfg(test)]
#[path = "side_pane_tests.rs"]
mod tests;
