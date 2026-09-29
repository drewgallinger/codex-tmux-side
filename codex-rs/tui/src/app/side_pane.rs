//! A side frontend owns its ephemeral thread; the main frontend only owns the tmux launch.

use super::event_dispatch::SHUTDOWN_FIRST_EXIT_TIMEOUT;
use super::*;
use crate::side_pane::SidePaneChild;
use crate::side_pane::SidePaneHandoff;
use crate::side_pane::SidePaneLaunch;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum SidePaneEvent {
    Started(Arc<SidePaneLaunch>),
    Forked(ThreadId),
    Ready,
    Closed,
    Failed(String),
}

pub(super) struct SidePaneState {
    pub(super) launch_id: Uuid,
    pub(super) launch: Option<Arc<SidePaneLaunch>>,
    thread_id: Option<ThreadId>,
    message: Option<crate::chatwidget::UserMessage>,
    ready: bool,
    watcher: Option<JoinHandle<()>>,
}

impl App {
    pub(super) fn side_pane_ignores_thread(&self, thread_id: ThreadId) -> bool {
        self.side_pane_ignored_threads.contains(&thread_id)
            || (self.side_pane_child.is_some() && self.primary_thread_id != Some(thread_id))
    }

    pub(super) async fn start_side_pane(
        &mut self,
        app_server: &mut AppServerSession,
        parent_thread_id: ThreadId,
        message: Option<crate::chatwidget::UserMessage>,
        source_pane: String,
    ) {
        if self.side_pane.as_ref().is_some_and(|state| !state.ready) {
            self.restore_side_user_message(message);
            self.chat_widget
                .add_error_message("A side conversation is still starting.".into());
            return;
        }
        self.close_side_pane(app_server).await;
        let prepared = (|| {
            let mut settings =
                app_server.side_fork_params(self.side_fork_config(), parent_thread_id);
            settings.thread_source = Some(codex_app_server_protocol::ThreadSource::Feature(
                "side_conversation".into(),
            ));
            let handoff = SidePaneHandoff::new(
                &self.app_server_target,
                parent_thread_id,
                settings,
                message.clone(),
            )?;
            let executable = self
                .daemon_cli_executable
                .as_ref()
                .map(|path| path.as_path().to_path_buf())
                .map(Ok)
                .unwrap_or_else(std::env::current_exe)?;
            Ok::<_, color_eyre::Report>((handoff, executable))
        })();
        let (handoff, executable) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.restore_side_user_message(message);
                self.chat_widget
                    .add_error_message(Self::side_start_error_message(&error));
                return;
            }
        };
        let launch_id = Uuid::new_v4();
        self.side_pane = Some(SidePaneState {
            launch_id,
            launch: None,
            thread_id: None,
            message,
            ready: false,
            watcher: None,
        });
        let events = self.app_event_tx.clone();
        let codex_home = self.config.codex_home.clone();
        let watcher = tokio::spawn(async move {
            let send = |event| events.send(AppEvent::SidePane { launch_id, event });
            let result = async {
                // Finish a started split even if the watcher is cancelled; an unclaimed
                // launch result drops its owned pane.
                let launch = Arc::new(
                    tokio::spawn(async move {
                        SidePaneLaunch::launch(&handoff, &executable, &source_pane, &codex_home)
                            .await
                    })
                    .await??,
                );
                send(SidePaneEvent::Started(Arc::clone(&launch)));
                send(SidePaneEvent::Forked(launch.wait_forked().await?));
                launch.wait_ready().await?;
                send(SidePaneEvent::Ready);
                launch.wait_closed().await?;
                send(SidePaneEvent::Closed);
                Ok::<_, color_eyre::Report>(())
            }
            .await;
            if let Err(error) = result {
                send(SidePaneEvent::Failed(error.to_string()));
            }
        });
        self.side_pane.as_mut().expect("side pane").watcher = Some(watcher);
    }

    pub(super) async fn handle_side_pane_event(
        &mut self,
        app_server: &mut AppServerSession,
        launch_id: Uuid,
        event: SidePaneEvent,
    ) {
        if !self
            .side_pane
            .as_ref()
            .is_some_and(|state| state.launch_id == launch_id)
        {
            if let SidePaneEvent::Started(launch) = event {
                let _ = launch.close().await;
            }
            return;
        }
        let result = async {
            match event {
                SidePaneEvent::Started(launch) => {
                    self.side_pane.as_mut().expect("side pane").launch = Some(launch);
                }
                SidePaneEvent::Forked(thread_id) => {
                    self.side_pane_ignored_threads.insert(thread_id);
                    self.discard_thread_local_state(thread_id).await;
                    let state = self.side_pane.as_mut().expect("side pane");
                    state.thread_id = Some(thread_id);
                    // Filtering is authoritative: automatic server subscription can race this.
                    let launch = Arc::clone(state.launch.as_ref().expect("launched pane"));
                    let request_handle = app_server.request_handle();
                    let request_id = app_server.next_request_id();
                    let events = self.app_event_tx.clone();
                    tokio::spawn(async move {
                        let result = async {
                            tokio::time::timeout(Duration::from_secs(/*secs*/ 5),
                                request_handle.request_typed::<codex_app_server_protocol::ThreadUnsubscribeResponse>(
                                    ClientRequest::ThreadUnsubscribe {
                                        request_id,
                                        params: codex_app_server_protocol::ThreadUnsubscribeParams {
                                            thread_id: thread_id.to_string(),
                                        },
                                    },
                                ),
                            ).await??;
                            launch.acknowledge_ownership()
                        }.await;
                        if let Err(error) = result {
                            events.send(AppEvent::SidePane {
                                launch_id,
                                event: SidePaneEvent::Failed(error.to_string()),
                            });
                        }
                    });
                }
                SidePaneEvent::Ready => {
                    let state = self.side_pane.as_mut().expect("side pane");
                    state.ready = true;
                    state.message = None;
                    state.launch.as_ref().expect("launched pane").focus().await?;
                }
                SidePaneEvent::Closed => {
                    let message = self.side_pane.as_mut().and_then(|state| state.message.take());
                    self.close_side_pane(app_server).await;
                    self.restore_side_user_message(message);
                }
                SidePaneEvent::Failed(message) => color_eyre::eyre::bail!("{message}"),
            }
            Ok::<_, color_eyre::Report>(())
        }.await;
        if let Err(error) = result {
            let message = self
                .side_pane
                .as_mut()
                .and_then(|state| state.message.take());
            self.close_side_pane(app_server).await;
            self.restore_side_user_message(message);
            self.chat_widget
                .add_error_message(Self::side_start_error_message(&error));
        }
    }

    pub(super) async fn close_side_pane(&mut self, app_server: &mut AppServerSession) {
        let Some(state) = self.side_pane.take() else {
            return;
        };
        if let Some(watcher) = state.watcher {
            watcher.abort();
        }
        let thread_id = state.thread_id.or_else(|| {
            state
                .launch
                .as_ref()
                .and_then(|launch| launch.forked_thread_id())
        });
        if let Some(thread_id) = thread_id {
            let _ = tokio::time::timeout(Duration::from_secs(/*secs*/ 1), async {
                if let Err(error) = app_server.startup_interrupt(thread_id).await {
                    tracing::warn!(%error, "failed to interrupt side pane thread");
                }
                if let Err(error) = app_server.thread_unsubscribe(thread_id).await {
                    tracing::warn!(%error, "failed to unsubscribe side pane thread");
                }
            })
            .await;
        }
        if let Some(launch) = state.launch {
            let _ = launch.close().await;
        }
    }

    pub(super) async fn start_side_pane_child(
        &mut self,
        _tui: &mut tui::Tui,
        app_server: &mut AppServerSession,
        handoff: SidePaneHandoff,
        child: SidePaneChild,
    ) -> Result<()> {
        self.side_pane_child = Some(child.clone());
        let result = async {
            let mut forked = app_server
                .fork_side_thread_with_params(
                    &self.local_settings,
                    self.config.clone(),
                    handoff.settings,
                )
                .await?;
            let thread_id = forked.session.thread_id;
            self.side_threads
                .insert(thread_id, SideThreadState::new(handoff.parent_thread_id));
            child.forked(thread_id)?;
            child.wait_for_ownership().await?;
            app_server
                .thread_inject_items(thread_id, vec![Self::side_boundary_prompt_item()])
                .await?;
            forked.session.forked_from_id = None;
            let service_tier = Some(forked.session.service_tier.clone().unwrap_or_else(|| {
                codex_protocol::config_types::SERVICE_TIER_DEFAULT_REQUEST_VALUE.to_string()
            }));
            self.enqueue_primary_thread_session(forked.session, Vec::new())
                .await?;
            self.config.service_tier = service_tier.clone();
            self.chat_widget.set_service_tier(service_tier);
            self.sync_side_thread_ui();
            // Readiness transfers the prepared draft to this frontend. From here, normal
            // submission error recovery belongs to the child; the parent must not replay it.
            child.ready()?;
            if let Some(message) = handoff.message {
                let _ = self
                    .chat_widget
                    .submit_user_message_as_plain_user_turn(message);
            }
            let events = self.app_event_tx.clone();
            let child = child.clone();
            tokio::spawn(async move {
                let _ = child.wait_parent_closed().await;
                events.send(AppEvent::Exit(ExitMode::ShutdownFirst));
            });
            Ok::<_, color_eyre::Report>(())
        }
        .await;
        if let Err(error) = &result {
            let _ = child.failed(&Self::side_start_error_message(error));
            let _ = tokio::time::timeout(
                SHUTDOWN_FIRST_EXIT_TIMEOUT,
                self.shutdown_side_threads(app_server),
            )
            .await;
        }
        result
    }
}

#[cfg(test)]
#[path = "side_pane_tests.rs"]
mod tests;
