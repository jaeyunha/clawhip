use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::Result;
use crate::events::MessageFormat;
use crate::source::tmux::{RegisteredTmuxSession, RegistrationSource};

const SCHEMA_VERSION: i64 = 2;
const DEFAULT_LEDGER_FILE: &str = "session-ledger.sqlite";

pub fn default_ledger_path() -> PathBuf {
    if let Some(path) = std::env::var_os("CLAWHIP_LEDGER") {
        return PathBuf::from(path);
    }
    default_state_dir().join(DEFAULT_LEDGER_FILE)
}

fn default_state_dir() -> PathBuf {
    std::env::var_os("CLAWHIP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".clawhip")))
        .unwrap_or_else(|| PathBuf::from(".clawhip"))
}

#[derive(Debug)]
pub struct Ledger {
    conn: Connection,
}

impl Ledger {
    pub fn open_default() -> Result<Self> {
        Self::open(default_ledger_path())
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        let ledger = Self { conn };
        ledger.migrate()?;
        Ok(ledger)
    }

    #[cfg(test)]
    fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let ledger = Self { conn };
        ledger.migrate()?;
        Ok(ledger)
    }

    pub fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;

            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                tmux_session TEXT NOT NULL UNIQUE,
                tmux_pane TEXT,
                kind TEXT NOT NULL,
                owner TEXT NOT NULL,
                project_path TEXT,
                repo_name TEXT,
                branch TEXT,
                spawned_by_clawhip INTEGER NOT NULL DEFAULT 0,
                expected_watch INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                workflow_status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_seen_at TEXT
            );

            CREATE TABLE IF NOT EXISTS watch_intents (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                channel TEXT,
                thread TEXT,
                mention TEXT,
                keywords_json TEXT NOT NULL,
                stale_minutes INTEGER NOT NULL,
                format TEXT,
                registration_source TEXT NOT NULL,
                original_command_json TEXT,
                registered_at TEXT NOT NULL,
                last_confirmed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_watch_intents_session_id
                ON watch_intents(session_id);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_watch_intents_session_unique
                ON watch_intents(session_id);

            CREATE TABLE IF NOT EXISTS github_issue_baselines (
                repo_path TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            "#,
        )?;
        self.ensure_column(
            "sessions",
            "workflow_status",
            "TEXT NOT NULL DEFAULT 'active'",
        )?;
        self.conn.execute(
            "UPDATE sessions SET workflow_status = 'retired' WHERE state = 'retired' AND workflow_status = 'active'",
            [],
        )?;
        self.conn.execute(
            "UPDATE sessions SET workflow_status = 'active' WHERE workflow_status IN ('failed', 'interrupted', 'abandoned')",
            [],
        )?;
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
            params![SCHEMA_VERSION, now_rfc3339()],
        )?;
        Ok(())
    }

    fn ensure_column(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        let mut statement = self.conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if !columns.iter().any(|existing| existing == column) {
            self.conn.execute(
                &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                [],
            )?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn applied_schema_versions(&self) -> Result<Vec<i64>> {
        let mut statement = self
            .conn
            .prepare("SELECT version FROM schema_migrations ORDER BY version")?;
        let rows = statement
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn github_issue_baseline(&self, repo_path: &str) -> Result<Option<String>> {
        let mut statement = self
            .conn
            .prepare("SELECT payload FROM github_issue_baselines WHERE repo_path = ?1")?;
        let mut rows = statement.query_map(params![repo_path], |row| row.get::<_, String>(0))?;
        match rows.next() {
            Some(payload) => Ok(Some(payload?)),
            None => Ok(None),
        }
    }

    pub fn set_github_issue_baseline(&self, repo_path: &str, payload: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO github_issue_baselines (repo_path, payload, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(repo_path) DO UPDATE SET
                 payload = excluded.payload,
                 updated_at = excluded.updated_at",
            params![repo_path, payload, now_rfc3339()],
        )?;
        Ok(())
    }

    pub fn upsert_session(&self, input: SessionInput) -> Result<SessionRecord> {
        input.validate()?;
        let now = now_rfc3339();
        let existing = self.session_by_name(&input.tmux_session)?;
        let id = existing
            .as_ref()
            .map(|session| session.id.clone())
            .unwrap_or_else(new_id);
        let created_at = existing
            .as_ref()
            .map(|session| session.created_at.clone())
            .unwrap_or_else(|| now.clone());
        self.conn.execute(
            r#"
            INSERT INTO sessions (
                id, tmux_session, tmux_pane, kind, owner, project_path, repo_name, branch,
                spawned_by_clawhip, expected_watch, state, workflow_status, created_at, updated_at, last_seen_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(tmux_session) DO UPDATE SET
                tmux_pane = excluded.tmux_pane,
                kind = excluded.kind,
                owner = excluded.owner,
                project_path = excluded.project_path,
                repo_name = excluded.repo_name,
                branch = excluded.branch,
                spawned_by_clawhip = CASE
                    WHEN sessions.spawned_by_clawhip != 0 OR excluded.spawned_by_clawhip != 0 THEN 1
                    ELSE 0
                END,
                expected_watch = excluded.expected_watch,
                state = excluded.state,
                workflow_status = excluded.workflow_status,
                updated_at = excluded.updated_at,
                last_seen_at = excluded.last_seen_at
            "#,
            params![
                id,
                input.tmux_session,
                input.tmux_pane,
                input.kind.as_str(),
                input.owner.as_str(),
                input.project_path,
                input.repo_name,
                input.branch,
                input.spawned_by_clawhip,
                input.expected_watch,
                input.state.as_str(),
                input.workflow_status.as_str(),
                created_at,
                now,
                input.last_seen_at,
            ],
        )?;
        self.session_by_name(&input.tmux_session)?
            .ok_or_else(|| anyhow!("session upsert did not produce a row").into())
    }

    pub fn upsert_watch_intent(&self, input: WatchIntentInput) -> Result<WatchIntentRecord> {
        input.validate()?;
        let session = self.session_by_name(&input.tmux_session)?.ok_or_else(|| {
            anyhow!(
                "cannot store watch intent for unknown session '{}'",
                input.tmux_session
            )
        })?;
        let id = self
            .watch_intent_for_session(&input.tmux_session)?
            .map(|intent| intent.id)
            .unwrap_or_else(new_id);
        let keywords_json = serde_json::to_string(&input.keywords)?;
        self.conn.execute(
            r#"
            INSERT INTO watch_intents (
                id, session_id, channel, thread, mention, keywords_json, stale_minutes,
                format, registration_source, original_command_json, registered_at, last_confirmed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            ON CONFLICT(session_id) DO UPDATE SET
                channel = excluded.channel,
                thread = excluded.thread,
                mention = excluded.mention,
                keywords_json = excluded.keywords_json,
                stale_minutes = excluded.stale_minutes,
                format = excluded.format,
                registration_source = excluded.registration_source,
                original_command_json = excluded.original_command_json,
                registered_at = excluded.registered_at,
                last_confirmed_at = excluded.last_confirmed_at
            "#,
            params![
                id,
                session.id,
                input.channel,
                input.thread,
                input.mention,
                keywords_json,
                input.stale_minutes,
                input.format.map(|format| format.as_str().to_string()),
                input.registration_source.as_str(),
                input.original_command_json,
                input.registered_at,
                input.last_confirmed_at,
            ],
        )?;
        self.watch_intent_for_session(&input.tmux_session)?
            .ok_or_else(|| anyhow!("watch intent upsert did not produce a row").into())
    }

    pub fn record_registration(
        &self,
        registration: &RegisteredTmuxSession,
        spawned_by_clawhip: bool,
        owner_mappings: &[crate::config::SessionOwnerMapping],
    ) -> Result<(SessionRecord, WatchIntentRecord)> {
        let session = self.upsert_session(SessionInput::from_registration(
            registration,
            spawned_by_clawhip,
            owner_mappings,
        ))?;
        let intent = self.upsert_watch_intent(WatchIntentInput::from_registration(registration))?;
        Ok((session, intent))
    }

    pub fn session_by_name(&self, tmux_session: &str) -> Result<Option<SessionRecord>> {
        self.conn
            .query_row(
                r#"
                SELECT id, tmux_session, tmux_pane, kind, owner, project_path, repo_name, branch,
                       spawned_by_clawhip, expected_watch, state, workflow_status, created_at, updated_at, last_seen_at
                FROM sessions
                WHERE tmux_session = ?1
                "#,
                params![tmux_session],
                map_session,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn watch_intent_for_session(
        &self,
        tmux_session: &str,
    ) -> Result<Option<WatchIntentRecord>> {
        self.conn
            .query_row(
                r#"
                SELECT wi.id, wi.session_id, s.tmux_session, wi.channel, wi.thread, wi.mention,
                       wi.keywords_json, wi.stale_minutes, wi.format, wi.registration_source,
                       wi.original_command_json, wi.registered_at, wi.last_confirmed_at
                FROM watch_intents wi
                JOIN sessions s ON s.id = wi.session_id
                WHERE s.tmux_session = ?1
                "#,
                params![tmux_session],
                map_watch_intent,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn watch_intents(&self) -> Result<Vec<WatchIntentRecord>> {
        let mut statement = self.conn.prepare(
            r#"
            SELECT wi.id, wi.session_id, s.tmux_session, wi.channel, wi.thread, wi.mention,
                   wi.keywords_json, wi.stale_minutes, wi.format, wi.registration_source,
                   wi.original_command_json, wi.registered_at, wi.last_confirmed_at
            FROM watch_intents wi
            JOIN sessions s ON s.id = wi.session_id
            ORDER BY s.tmux_session
            "#,
        )?;
        Ok(statement
            .query_map([], map_watch_intent)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn sessions(&self) -> Result<Vec<SessionRecord>> {
        let mut statement = self.conn.prepare(
            r#"
            SELECT id, tmux_session, tmux_pane, kind, owner, project_path, repo_name, branch,
                   spawned_by_clawhip, expected_watch, state, workflow_status, created_at, updated_at, last_seen_at
            FROM sessions
            ORDER BY tmux_session
            "#,
        )?;
        Ok(statement
            .query_map([], map_session)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn set_session_state(
        &self,
        tmux_session: &str,
        state: SessionState,
    ) -> Result<SessionRecord> {
        self.conn.execute(
            "UPDATE sessions SET state = ?1, updated_at = ?2 WHERE tmux_session = ?3",
            params![state.as_str(), now_rfc3339(), tmux_session],
        )?;
        self.session_by_name(tmux_session)?
            .ok_or_else(|| anyhow!("unknown session '{tmux_session}'").into())
    }

    pub fn set_workflow_status(
        &self,
        tmux_session: &str,
        workflow_status: WorkflowStatus,
    ) -> Result<SessionRecord> {
        self.conn.execute(
            "UPDATE sessions SET workflow_status = ?1, updated_at = ?2 WHERE tmux_session = ?3",
            params![workflow_status.as_str(), now_rfc3339(), tmux_session],
        )?;
        self.session_by_name(tmux_session)?
            .ok_or_else(|| anyhow!("unknown session '{tmux_session}'").into())
    }

    pub fn mark_ignored(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_session_state(tmux_session, SessionState::IgnoredAlive)
    }

    pub fn complete_session(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::Completed)
    }

    pub fn mark_needs_review(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::NeedsReview)
    }

    pub fn mark_needs_qa(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::NeedsQa)
    }

    pub fn mark_pr_open(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::PrOpen)
    }

    pub fn mark_awaiting_ci(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::AwaitingCi)
    }

    pub fn mark_awaiting_human(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::AwaitingHuman)
    }

    pub fn cancel_session(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::Cancelled)
    }

    pub fn supersede_session(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::Superseded)
    }

    pub fn retire_session(&self, tmux_session: &str) -> Result<SessionRecord> {
        self.set_workflow_status(tmux_session, WorkflowStatus::Retired)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LaneStatus {
    HealthyAgent,
    LostMonitoring,
    IgnoredInfra,
    UnknownTmux,
    InfraCandidate,
    ExternallyWatched,
    WorkflowHandoff,
    TmuxMissing,
}

impl LaneStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HealthyAgent => "healthy-agent",
            Self::LostMonitoring => "lost-monitoring",
            Self::IgnoredInfra => "ignored-infra",
            Self::UnknownTmux => "unknown-tmux",
            Self::InfraCandidate => "infra-candidate",
            Self::ExternallyWatched => "externally-watched",
            Self::WorkflowHandoff => "workflow-handoff",
            Self::TmuxMissing => "tmux-missing",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowStatus {
    Active,
    NeedsReview,
    NeedsQa,
    PrOpen,
    AwaitingCi,
    AwaitingHuman,
    Completed,
    Superseded,
    Cancelled,
    Retired,
}

impl WorkflowStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::NeedsReview => "needs-review",
            Self::NeedsQa => "needs-qa",
            Self::PrOpen => "pr-open",
            Self::AwaitingCi => "awaiting-ci",
            Self::AwaitingHuman => "awaiting-human",
            Self::Completed => "completed",
            Self::Superseded => "superseded",
            Self::Cancelled => "cancelled",
            Self::Retired => "retired",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Superseded | Self::Cancelled | Self::Retired
        )
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "needs-review" => Ok(Self::NeedsReview),
            "needs-qa" => Ok(Self::NeedsQa),
            "pr-open" => Ok(Self::PrOpen),
            "awaiting-ci" => Ok(Self::AwaitingCi),
            "awaiting-human" => Ok(Self::AwaitingHuman),
            "completed" => Ok(Self::Completed),
            "failed" | "interrupted" | "abandoned" => Ok(Self::Active),
            "superseded" => Ok(Self::Superseded),
            "cancelled" => Ok(Self::Cancelled),
            "retired" => Ok(Self::Retired),
            _ => Err(anyhow!("invalid workflow status '{value}'").into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeStatus {
    Live,
    TmuxMissing,
}

impl RuntimeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::TmuxMissing => "tmux-missing",
        }
    }

    fn from_live_tmux(live_tmux: bool) -> Self {
        if live_tmux {
            Self::Live
        } else {
            Self::TmuxMissing
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LaneRow {
    pub session: String,
    pub status: LaneStatus,
    pub workflow_status: WorkflowStatus,
    pub runtime_status: RuntimeStatus,
    pub kind: SessionKind,
    pub owner: SessionOwner,
    pub state: SessionState,
    pub spawned_by_clawhip: bool,
    pub expected_watch: bool,
    pub registration_source: Option<RegistrationSource>,
    pub live_tmux: bool,
    pub daemon_watch: bool,
    pub has_restore: bool,
    pub channel: Option<String>,
    pub thread: Option<String>,
    pub repo_name: Option<String>,
    pub branch: Option<String>,
    pub restore_command: Option<String>,
}

pub fn classify_lanes(
    ledger_sessions: &[SessionRecord],
    watch_intents: &[WatchIntentRecord],
    daemon_registrations: &[RegisteredTmuxSession],
    live_tmux_sessions: &BTreeSet<String>,
    infra_session_prefixes: &[String],
) -> Vec<LaneRow> {
    let intent_by_session = watch_intents
        .iter()
        .map(|intent| (intent.tmux_session.as_str(), intent))
        .collect::<BTreeMap<_, _>>();
    let daemon_by_session = daemon_registrations
        .iter()
        .map(|registration| (registration.session.as_str(), registration))
        .collect::<BTreeMap<_, _>>();

    let mut rows = Vec::new();
    let mut known_sessions = BTreeSet::new();

    for session in ledger_sessions {
        known_sessions.insert(session.tmux_session.clone());
        if session.workflow_status.is_terminal() || session.state == SessionState::Retired {
            continue;
        }
        let live_tmux = live_tmux_sessions.contains(&session.tmux_session);
        let daemon_registration = daemon_by_session
            .get(session.tmux_session.as_str())
            .copied();
        let daemon_watch = daemon_registration.is_some();
        let intent = intent_by_session
            .get(session.tmux_session.as_str())
            .copied();
        let status = classify_ledger_session(session, live_tmux, daemon_watch);
        rows.push(LaneRow {
            session: session.tmux_session.clone(),
            status,
            workflow_status: session.workflow_status,
            runtime_status: RuntimeStatus::from_live_tmux(live_tmux),
            kind: session.kind,
            owner: session.owner.clone(),
            state: session.state,
            spawned_by_clawhip: session.spawned_by_clawhip,
            expected_watch: session.expected_watch,
            registration_source: intent.map(|intent| intent.registration_source),
            live_tmux,
            daemon_watch,
            has_restore: intent.is_some(),
            channel: intent
                .and_then(|intent| intent.channel.clone())
                .or_else(|| {
                    daemon_registration.and_then(|registration| registration.channel.clone())
                }),
            thread: intent.and_then(|intent| intent.thread.clone()).or_else(|| {
                daemon_registration.and_then(|registration| registration.thread.clone())
            }),
            repo_name: session.repo_name.clone(),
            branch: session.branch.clone(),
            restore_command: intent.map(WatchIntentRecord::restore_command_string),
        });
    }

    for registration in daemon_registrations {
        if known_sessions.contains(&registration.session) {
            continue;
        }
        known_sessions.insert(registration.session.clone());
        rows.push(LaneRow {
            session: registration.session.clone(),
            status: LaneStatus::ExternallyWatched,
            workflow_status: WorkflowStatus::Active,
            runtime_status: RuntimeStatus::from_live_tmux(
                live_tmux_sessions.contains(&registration.session),
            ),
            kind: SessionKind::Unknown,
            owner: SessionOwner::Unknown,
            state: SessionState::Healthy,
            spawned_by_clawhip: false,
            expected_watch: true,
            registration_source: Some(registration.registration_source),
            live_tmux: live_tmux_sessions.contains(&registration.session),
            daemon_watch: true,
            has_restore: false,
            channel: registration.channel.clone(),
            thread: registration.thread.clone(),
            repo_name: registration.routing.repo_name.clone(),
            branch: registration.routing.branch.clone(),
            restore_command: None,
        });
    }

    for session in live_tmux_sessions {
        if known_sessions.contains(session) {
            continue;
        }
        rows.push(LaneRow {
            session: session.clone(),
            status: classify_unclaimed_tmux_session(session, infra_session_prefixes),
            workflow_status: WorkflowStatus::Active,
            runtime_status: RuntimeStatus::Live,
            kind: SessionKind::Unknown,
            owner: SessionOwner::Unknown,
            state: SessionState::Unknown,
            spawned_by_clawhip: false,
            expected_watch: false,
            registration_source: None,
            live_tmux: true,
            daemon_watch: false,
            has_restore: false,
            channel: None,
            thread: None,
            repo_name: None,
            branch: None,
            restore_command: None,
        });
    }

    rows.sort_by(|left, right| left.session.cmp(&right.session));
    rows
}

fn classify_ledger_session(
    session: &SessionRecord,
    live_tmux: bool,
    daemon_watch: bool,
) -> LaneStatus {
    if !live_tmux {
        return if session.workflow_status == WorkflowStatus::Active {
            LaneStatus::TmuxMissing
        } else {
            LaneStatus::WorkflowHandoff
        };
    }
    if session.state == SessionState::IgnoredAlive {
        return LaneStatus::IgnoredInfra;
    }
    if session.expected_watch && daemon_watch {
        return LaneStatus::HealthyAgent;
    }
    if session.expected_watch && !daemon_watch {
        return LaneStatus::LostMonitoring;
    }
    if !session.expected_watch && daemon_watch {
        return LaneStatus::ExternallyWatched;
    }
    LaneStatus::UnknownTmux
}

fn classify_unclaimed_tmux_session(session: &str, infra_prefixes: &[String]) -> LaneStatus {
    if infra_prefixes
        .iter()
        .any(|prefix| session.starts_with(prefix.as_str()))
    {
        LaneStatus::InfraCandidate
    } else {
        LaneStatus::UnknownTmux
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionKind {
    Agent,
    Infra,
    Human,
    Unknown,
}

impl SessionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Infra => "infra",
            Self::Human => "human",
            Self::Unknown => "unknown",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "agent" => Ok(Self::Agent),
            "infra" => Ok(Self::Infra),
            "human" => Ok(Self::Human),
            "unknown" => Ok(Self::Unknown),
            _ => Err(anyhow!("invalid session kind '{value}'").into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SessionOwner {
    System,
    Unknown,
    Named(String),
}

impl SessionOwner {
    pub fn as_str(&self) -> &str {
        match self {
            Self::System => "system",
            Self::Unknown => "unknown",
            Self::Named(name) => name.as_str(),
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "system" => Ok(Self::System),
            "unknown" => Ok(Self::Unknown),
            "" => Err(anyhow!("session owner cannot be empty").into()),
            other => Ok(Self::Named(other.to_string())),
        }
    }

    fn infer_from_registration(
        registration: &RegisteredTmuxSession,
        mappings: &[crate::config::SessionOwnerMapping],
    ) -> Self {
        let repo = match registration.routing.repo_name.as_deref() {
            Some(r) => r,
            None => return Self::Unknown,
        };
        for mapping in mappings {
            if mapping.repo == repo {
                return Self::Named(mapping.owner.clone());
            }
        }
        Self::Unknown
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionState {
    Unknown,
    Healthy,
    LostMonitoring,
    IgnoredAlive,
    Dead,
    Retired,
}

impl SessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::LostMonitoring => "lost-monitoring",
            Self::IgnoredAlive => "ignored-alive",
            Self::Dead => "dead",
            Self::Retired => "retired",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "unknown" => Ok(Self::Unknown),
            "healthy" => Ok(Self::Healthy),
            "lost-monitoring" => Ok(Self::LostMonitoring),
            "ignored-alive" => Ok(Self::IgnoredAlive),
            "dead" => Ok(Self::Dead),
            "retired" => Ok(Self::Retired),
            _ => Err(anyhow!("invalid session state '{value}'").into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionInput {
    pub tmux_session: String,
    pub tmux_pane: Option<String>,
    pub kind: SessionKind,
    pub owner: SessionOwner,
    pub project_path: Option<String>,
    pub repo_name: Option<String>,
    pub branch: Option<String>,
    pub spawned_by_clawhip: bool,
    pub expected_watch: bool,
    pub state: SessionState,
    pub workflow_status: WorkflowStatus,
    pub last_seen_at: Option<String>,
}

impl SessionInput {
    fn from_registration(
        registration: &RegisteredTmuxSession,
        spawned_by_clawhip: bool,
        owner_mappings: &[crate::config::SessionOwnerMapping],
    ) -> Self {
        Self {
            tmux_session: registration.session.clone(),
            tmux_pane: None,
            kind: SessionKind::Agent,
            owner: SessionOwner::infer_from_registration(registration, owner_mappings),
            project_path: registration.routing.worktree_path.clone(),
            repo_name: registration.routing.repo_name.clone(),
            branch: registration.routing.branch.clone(),
            spawned_by_clawhip,
            expected_watch: true,
            state: SessionState::Unknown,
            workflow_status: WorkflowStatus::Active,
            last_seen_at: Some(now_rfc3339()),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.tmux_session.trim().is_empty() {
            return Err(anyhow!("tmux session cannot be empty").into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionRecord {
    pub id: String,
    pub tmux_session: String,
    pub tmux_pane: Option<String>,
    pub kind: SessionKind,
    pub owner: SessionOwner,
    pub project_path: Option<String>,
    pub repo_name: Option<String>,
    pub branch: Option<String>,
    pub spawned_by_clawhip: bool,
    pub expected_watch: bool,
    pub state: SessionState,
    pub workflow_status: WorkflowStatus,
    pub created_at: String,
    pub updated_at: String,
    pub last_seen_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WatchIntentInput {
    pub tmux_session: String,
    pub channel: Option<String>,
    pub thread: Option<String>,
    pub mention: Option<String>,
    pub keywords: Vec<String>,
    pub stale_minutes: u64,
    pub format: Option<MessageFormat>,
    pub registration_source: RegistrationSource,
    pub original_command_json: Option<String>,
    pub registered_at: String,
    pub last_confirmed_at: Option<String>,
}

impl WatchIntentInput {
    fn from_registration(registration: &RegisteredTmuxSession) -> Self {
        Self {
            tmux_session: registration.session.clone(),
            channel: registration.channel.clone(),
            thread: registration.thread.clone(),
            mention: registration.mention.clone(),
            keywords: registration.keywords.clone(),
            stale_minutes: registration.stale_minutes,
            format: registration.format.clone(),
            registration_source: registration.registration_source,
            original_command_json: serde_json::to_string(registration).ok(),
            registered_at: registration.registered_at.clone(),
            last_confirmed_at: Some(now_rfc3339()),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.tmux_session.trim().is_empty() {
            return Err(anyhow!("tmux session cannot be empty").into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WatchIntentRecord {
    pub id: String,
    pub session_id: String,
    pub tmux_session: String,
    pub channel: Option<String>,
    pub thread: Option<String>,
    pub mention: Option<String>,
    pub keywords: Vec<String>,
    pub stale_minutes: u64,
    pub format: Option<MessageFormat>,
    pub registration_source: RegistrationSource,
    pub original_command_json: Option<String>,
    pub registered_at: String,
    pub last_confirmed_at: Option<String>,
}

impl WatchIntentRecord {
    pub fn restore_command(&self) -> Vec<String> {
        let mut args = vec![
            "clawhip".to_string(),
            "tmux".to_string(),
            "watch".to_string(),
            "--session".to_string(),
            self.tmux_session.clone(),
        ];
        if let Some(channel) = &self.channel {
            args.extend(["--channel".to_string(), channel.clone()]);
        }
        if let Some(thread) = &self.thread {
            args.extend(["--thread".to_string(), thread.clone()]);
        }
        if let Some(mention) = &self.mention {
            args.extend(["--mention".to_string(), mention.clone()]);
        }
        if !self.keywords.is_empty() {
            args.extend(["--keywords".to_string(), self.keywords.join(",")]);
        }
        args.extend([
            "--stale-minutes".to_string(),
            self.stale_minutes.to_string(),
        ]);
        if let Some(format) = &self.format {
            args.extend(["--format".to_string(), format.as_str().to_string()]);
        }
        args
    }

    pub fn restore_command_string(&self) -> String {
        self.restore_command()
            .iter()
            .map(|arg| shell_quote(arg))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn map_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    let kind: String = row.get(3)?;
    let owner: String = row.get(4)?;
    let state: String = row.get(10)?;
    let workflow_status: String = row.get(11)?;
    Ok(SessionRecord {
        id: row.get(0)?,
        tmux_session: row.get(1)?,
        tmux_pane: row.get(2)?,
        kind: SessionKind::parse(&kind).map_err(to_sql_dyn_error)?,
        owner: SessionOwner::parse(&owner).map_err(to_sql_dyn_error)?,
        project_path: row.get(5)?,
        repo_name: row.get(6)?,
        branch: row.get(7)?,
        spawned_by_clawhip: row.get(8)?,
        expected_watch: row.get(9)?,
        state: SessionState::parse(&state).map_err(to_sql_dyn_error)?,
        workflow_status: WorkflowStatus::parse(&workflow_status).map_err(to_sql_dyn_error)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
        last_seen_at: row.get(14)?,
    })
}

fn map_watch_intent(row: &rusqlite::Row<'_>) -> rusqlite::Result<WatchIntentRecord> {
    let keywords_json: String = row.get(6)?;
    let keywords = serde_json::from_str::<Vec<String>>(&keywords_json).map_err(to_sql_error)?;
    let format = row
        .get::<_, Option<String>>(8)?
        .map(|format| parse_message_format(&format))
        .transpose()
        .map_err(to_sql_dyn_error)?;
    let registration_source = RegistrationSourceSerde::parse(&row.get::<_, String>(9)?)
        .map_err(to_sql_dyn_error)?
        .0;
    Ok(WatchIntentRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        tmux_session: row.get(2)?,
        channel: row.get(3)?,
        thread: row.get(4)?,
        mention: row.get(5)?,
        keywords,
        stale_minutes: row.get::<_, i64>(7)? as u64,
        format,
        registration_source,
        original_command_json: row.get(10)?,
        registered_at: row.get(11)?,
        last_confirmed_at: row.get(12)?,
    })
}

struct RegistrationSourceSerde(RegistrationSource);

impl RegistrationSourceSerde {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "cli-watch" => Ok(Self(RegistrationSource::CliWatch)),
            "cli-new" => Ok(Self(RegistrationSource::CliNew)),
            "config-monitor" => Ok(Self(RegistrationSource::ConfigMonitor)),
            _ => Err(anyhow!("invalid registration source '{value}'").into()),
        }
    }
}

fn parse_message_format(value: &str) -> Result<MessageFormat> {
    MessageFormat::from_label(value)
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn new_id() -> String {
    Uuid::new_v4().to_string()
}

fn to_sql_error<E>(error: E) -> rusqlite::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn to_sql_dyn_error(error: crate::DynError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(error)
}

fn shell_quote(arg: &str) -> String {
    crate::shell::shell_quote(arg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{MessageFormat, RoutingMetadata};

    fn sample_session_input(name: &str) -> SessionInput {
        SessionInput {
            tmux_session: name.to_string(),
            tmux_pane: Some("0.0".to_string()),
            kind: SessionKind::Agent,
            owner: SessionOwner::Named("alice".to_string()),
            project_path: Some("/tmp/project".to_string()),
            repo_name: Some("project".to_string()),
            branch: Some("main".to_string()),
            spawned_by_clawhip: true,
            expected_watch: true,
            state: SessionState::Unknown,
            workflow_status: WorkflowStatus::Active,
            last_seen_at: Some("2026-06-04T00:00:00Z".to_string()),
        }
    }

    fn sample_watch_input(name: &str) -> WatchIntentInput {
        WatchIntentInput {
            tmux_session: name.to_string(),
            channel: Some("alerts".to_string()),
            thread: None,
            mention: Some("<@123>".to_string()),
            keywords: vec!["READY_FOR_REVIEW".to_string(), "BLOCKED".to_string()],
            stale_minutes: 10,
            format: Some(MessageFormat::Alert),
            registration_source: RegistrationSource::CliNew,
            original_command_json: Some(r#"{"session":"agent-1"}"#.to_string()),
            registered_at: "2026-06-04T00:00:00Z".to_string(),
            last_confirmed_at: Some("2026-06-04T00:00:01Z".to_string()),
        }
    }

    #[test]
    fn migrations_are_idempotent() {
        let ledger = Ledger::open_memory().expect("ledger");
        ledger.migrate().expect("second migration");
        assert_eq!(
            ledger.applied_schema_versions().unwrap(),
            vec![SCHEMA_VERSION]
        );
    }

    #[test]
    fn github_issue_baseline_round_trip() {
        let ledger = Ledger::open_memory().expect("ledger");
        assert_eq!(ledger.github_issue_baseline("/tmp/repo").unwrap(), None);
        ledger
            .set_github_issue_baseline("/tmp/repo", "{\"high_water_mark\":3}")
            .expect("insert baseline");
        assert_eq!(
            ledger
                .github_issue_baseline("/tmp/repo")
                .unwrap()
                .as_deref(),
            Some("{\"high_water_mark\":3}")
        );
        ledger
            .set_github_issue_baseline("/tmp/repo", "{\"high_water_mark\":7}")
            .expect("overwrite baseline");
        assert_eq!(
            ledger
                .github_issue_baseline("/tmp/repo")
                .unwrap()
                .as_deref(),
            Some("{\"high_water_mark\":7}")
        );
    }

    #[test]
    fn upserts_and_reads_session() {
        let ledger = Ledger::open_memory().expect("ledger");
        let first = ledger
            .upsert_session(sample_session_input("agent-1"))
            .expect("insert session");
        let mut update = sample_session_input("agent-1");
        update.state = SessionState::Healthy;
        update.branch = Some("feature".to_string());
        let second = ledger.upsert_session(update).expect("update session");

        assert_eq!(first.id, second.id);
        assert_eq!(second.state, SessionState::Healthy);
        assert_eq!(second.branch.as_deref(), Some("feature"));
        assert_eq!(ledger.sessions().unwrap().len(), 1);
    }

    #[test]
    fn upserts_watch_intent_without_duplicates() {
        let ledger = Ledger::open_memory().expect("ledger");
        ledger
            .upsert_session(sample_session_input("agent-1"))
            .expect("session");
        let first = ledger
            .upsert_watch_intent(sample_watch_input("agent-1"))
            .expect("insert watch");
        let mut update = sample_watch_input("agent-1");
        update.keywords = vec!["DONE".to_string()];
        update.stale_minutes = 5;
        let second = ledger.upsert_watch_intent(update).expect("update watch");

        assert_eq!(first.id, second.id);
        assert_eq!(second.keywords, vec!["DONE"]);
        assert_eq!(second.stale_minutes, 5);
    }

    #[test]
    fn restore_command_renders_safe_watch_command() {
        let ledger = Ledger::open_memory().expect("ledger");
        ledger
            .upsert_session(sample_session_input("agent-1"))
            .expect("session");
        let intent = ledger
            .upsert_watch_intent(sample_watch_input("agent-1"))
            .expect("watch");

        assert_eq!(
            intent.restore_command(),
            vec![
                "clawhip",
                "tmux",
                "watch",
                "--session",
                "agent-1",
                "--channel",
                "alerts",
                "--mention",
                "<@123>",
                "--keywords",
                "READY_FOR_REVIEW,BLOCKED",
                "--stale-minutes",
                "10",
                "--format",
                "alert"
            ]
        );
        assert!(
            intent
                .restore_command_string()
                .contains("--mention '<@123>'")
        );
        assert!(
            intent
                .restore_command_string()
                .contains("--session agent-1")
        );
    }

    fn owner_mapping(repo: &str, owner: &str) -> crate::config::SessionOwnerMapping {
        crate::config::SessionOwnerMapping {
            repo: repo.to_string(),
            owner: owner.to_string(),
        }
    }

    #[test]
    fn record_registration_captures_routing_metadata() {
        let ledger = Ledger::open_memory().expect("ledger");
        let registration = RegisteredTmuxSession {
            session: "agent-2".to_string(),
            channel: Some("channel-1".to_string()),
            thread: None,
            mention: None,
            routing: RoutingMetadata {
                repo_name: Some("forever-agent".to_string()),
                worktree_path: Some("/tmp/forever-agent".to_string()),
                branch: Some("main".to_string()),
                ..RoutingMetadata::default()
            },
            keywords: vec!["READY".to_string()],
            keyword_window_secs: 30,
            stale_minutes: 12,
            format: Some(MessageFormat::Compact),
            registered_at: "2026-06-04T00:00:00Z".to_string(),
            registration_source: RegistrationSource::CliNew,
            parent_process: None,
            active_wrapper_monitor: false,
        };
        let mappings = vec![owner_mapping("forever-agent", "walter")];

        let (session, intent) = ledger
            .record_registration(&registration, true, &mappings)
            .expect("record");
        assert_eq!(session.kind, SessionKind::Agent);
        assert_eq!(session.owner, SessionOwner::Named("walter".to_string()));
        assert!(session.spawned_by_clawhip);
        assert!(session.expected_watch);
        assert_eq!(session.repo_name.as_deref(), Some("forever-agent"));
        assert_eq!(intent.registration_source, RegistrationSource::CliNew);
        assert_eq!(intent.keywords, vec!["READY"]);
    }

    #[test]
    fn cli_watch_reregistration_preserves_clawhip_spawn_provenance() {
        let ledger = Ledger::open_memory().expect("ledger");
        let initial_registration = registration("agent-reregistered");
        let (session, _) = ledger
            .record_registration(&initial_registration, true, &[])
            .expect("record initial clawhip spawn");

        assert!(session.spawned_by_clawhip);

        let mut watch_registration = registration("agent-reregistered");
        watch_registration.registration_source = RegistrationSource::CliWatch;
        watch_registration.keywords =
            vec!["https://github.com/namuh-eng/opensend/pull/".to_string()];
        watch_registration.thread = Some("thread-123".to_string());

        let (session, intent) = ledger
            .record_registration(&watch_registration, false, &[])
            .expect("record reregistration");

        assert!(session.spawned_by_clawhip);
        assert_eq!(intent.registration_source, RegistrationSource::CliWatch);
        assert_eq!(
            intent.keywords,
            vec!["https://github.com/namuh-eng/opensend/pull/"]
        );
        assert_eq!(intent.thread.as_deref(), Some("thread-123"));
    }

    #[test]
    fn record_registration_config_driven_owner_mapping() {
        let ledger = Ledger::open_memory().expect("ledger");
        let mappings = vec![
            owner_mapping("opensend", "hermes"),
            owner_mapping("forgeos", "hermes"),
        ];
        for repo_name in ["opensend", "forgeos"] {
            let mut reg = registration(repo_name);
            reg.routing.repo_name = Some(repo_name.to_string());

            let (session, _) = ledger
                .record_registration(&reg, true, &mappings)
                .expect("record");

            assert_eq!(
                session.owner,
                SessionOwner::Named("hermes".to_string()),
                "repo {repo_name}"
            );
            assert_eq!(session.repo_name.as_deref(), Some(repo_name));
        }
    }

    #[test]
    fn old_db_owner_values_parse_as_named() {
        // Old DB rows like owner="walter" or owner="hermes-orchestrator" must
        // round-trip through parse() as Named(...), not error.
        let walter = SessionOwner::parse("walter").expect("parse walter");
        assert_eq!(walter, SessionOwner::Named("walter".to_string()));
        assert_eq!(walter.as_str(), "walter");

        let hermes = SessionOwner::parse("hermes-orchestrator").expect("parse hermes");
        assert_eq!(
            hermes,
            SessionOwner::Named("hermes-orchestrator".to_string())
        );

        let system = SessionOwner::parse("system").expect("parse system");
        assert_eq!(system, SessionOwner::System);

        let unknown = SessionOwner::parse("unknown").expect("parse unknown");
        assert_eq!(unknown, SessionOwner::Unknown);

        assert!(SessionOwner::parse("").is_err());
    }

    #[test]
    fn record_registration_uses_unknown_owner_for_unmapped_projects() {
        let ledger = Ledger::open_memory().expect("ledger");
        let mut registration = registration("agent-unknown");
        registration.routing.repo_name = Some("side-project".to_string());

        let (session, _) = ledger
            .record_registration(&registration, true, &[])
            .expect("record");

        assert_eq!(session.owner, SessionOwner::Unknown);
    }

    #[test]
    fn rejects_empty_session_names() {
        let ledger = Ledger::open_memory().expect("ledger");
        let error = ledger
            .upsert_session(sample_session_input(" "))
            .expect_err("empty session should fail");
        assert!(error.to_string().contains("tmux session cannot be empty"));
    }

    #[test]
    fn classifies_expected_lane_states() {
        let mut healthy = sample_session_input("healthy");
        healthy.state = SessionState::Healthy;
        let mut lost = sample_session_input("lost");
        lost.state = SessionState::LostMonitoring;
        let mut ignored = sample_session_input("infra");
        ignored.kind = SessionKind::Infra;
        ignored.owner = SessionOwner::System;
        ignored.expected_watch = false;
        ignored.state = SessionState::IgnoredAlive;
        let mut dead = sample_session_input("dead");
        dead.state = SessionState::Dead;
        let ledger = Ledger::open_memory().expect("ledger");
        for input in [healthy, lost, ignored, dead] {
            ledger.upsert_session(input).expect("session");
        }
        ledger
            .upsert_watch_intent(sample_watch_input("healthy"))
            .expect("healthy watch");
        ledger
            .upsert_watch_intent(sample_watch_input("lost"))
            .expect("lost watch");
        let mut external_registration = registration("external");
        external_registration.registration_source = RegistrationSource::CliWatch;
        let daemon_registrations = vec![registration("healthy"), external_registration];
        let live_tmux_sessions = [
            "ever-forever-agent",
            "external",
            "healthy",
            "infra",
            "lost",
            "manual",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();

        let infra_prefixes = vec!["ever-".to_string()];
        let rows = classify_lanes(
            &ledger.sessions().unwrap(),
            &ledger.watch_intents().unwrap(),
            &daemon_registrations,
            &live_tmux_sessions,
            &infra_prefixes,
        );
        let statuses = rows
            .iter()
            .map(|row| (row.session.as_str(), row.status))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(statuses["healthy"], LaneStatus::HealthyAgent);
        assert_eq!(statuses["lost"], LaneStatus::LostMonitoring);
        assert_eq!(statuses["infra"], LaneStatus::IgnoredInfra);
        assert_eq!(statuses["manual"], LaneStatus::UnknownTmux);
        assert_eq!(statuses["ever-forever-agent"], LaneStatus::InfraCandidate);
        assert_eq!(statuses["external"], LaneStatus::ExternallyWatched);
        assert_eq!(statuses["dead"], LaneStatus::TmuxMissing);
    }

    #[test]
    fn missing_runtime_with_pending_workflow_is_a_handoff() {
        let ledger = Ledger::open_memory().expect("ledger");
        for (name, workflow_status) in [
            ("review", WorkflowStatus::NeedsReview),
            ("qa", WorkflowStatus::NeedsQa),
            ("pr", WorkflowStatus::PrOpen),
            ("ci", WorkflowStatus::AwaitingCi),
            ("human", WorkflowStatus::AwaitingHuman),
        ] {
            let mut input = sample_session_input(name);
            input.state = SessionState::Dead;
            input.workflow_status = workflow_status;
            ledger.upsert_session(input).expect("session");
        }

        let rows = classify_lanes(&ledger.sessions().unwrap(), &[], &[], &BTreeSet::new(), &[]);

        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|row| {
            row.status == LaneStatus::WorkflowHandoff
                && row.runtime_status == RuntimeStatus::TmuxMissing
        }));
    }

    #[test]
    fn terminal_sessions_are_hidden_from_lane_board() {
        let ledger = Ledger::open_memory().expect("ledger");
        for (name, workflow_status) in [
            ("completed-review", WorkflowStatus::Completed),
            ("cancelled-review", WorkflowStatus::Cancelled),
            ("superseded-review", WorkflowStatus::Superseded),
            ("retired-review", WorkflowStatus::Retired),
        ] {
            let mut input = sample_session_input(name);
            input.workflow_status = workflow_status;
            ledger.upsert_session(input).expect("session");
            ledger
                .upsert_watch_intent(sample_watch_input(name))
                .expect("watch");
        }

        let rows = classify_lanes(
            &ledger.sessions().unwrap(),
            &ledger.watch_intents().unwrap(),
            &[],
            &BTreeSet::new(),
            &[],
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn ignored_session_becomes_tmux_missing_when_tmux_disappears() {
        let ledger = Ledger::open_memory().expect("ledger");
        let mut ignored = sample_session_input("infra");
        ignored.expected_watch = false;
        ignored.state = SessionState::IgnoredAlive;
        ledger.upsert_session(ignored).expect("session");

        let rows = classify_lanes(
            &ledger.sessions().unwrap(),
            &ledger.watch_intents().unwrap(),
            &[],
            &BTreeSet::new(),
            &[],
        );

        assert_eq!(rows[0].status, LaneStatus::TmuxMissing);
    }

    #[test]
    fn record_registration_persists_thread_restore_target() {
        let ledger = Ledger::open_memory().expect("ledger");
        let mut registration = registration("agent-threaded");
        registration.channel = None;
        registration.thread = Some("thread-123".to_string());

        let (_session, intent) = ledger
            .record_registration(&registration, true, &[])
            .expect("registration");

        assert_eq!(intent.channel, None);
        assert_eq!(intent.thread.as_deref(), Some("thread-123"));
        assert!(
            intent
                .restore_command_string()
                .contains("--thread thread-123"),
            "{}",
            intent.restore_command_string()
        );
    }

    fn registration(session: &str) -> RegisteredTmuxSession {
        RegisteredTmuxSession {
            session: session.to_string(),
            channel: Some("alerts".to_string()),
            thread: None,
            mention: None,
            routing: RoutingMetadata::default(),
            keywords: vec!["READY".to_string()],
            keyword_window_secs: 30,
            stale_minutes: 10,
            format: Some(MessageFormat::Compact),
            registered_at: "2026-06-04T00:00:00Z".to_string(),
            registration_source: RegistrationSource::CliNew,
            parent_process: None,
            active_wrapper_monitor: false,
        }
    }
}
