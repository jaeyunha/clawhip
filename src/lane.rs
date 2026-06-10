use std::path::{Path, PathBuf};
use std::process::Command;

use crate::Result;
use crate::cli::{LaneInspectArgs, LaneVerifyArgs, LaneWorktreesArgs};
use crate::config::AppConfig;
use crate::source::tmux::tmux_bin;

pub async fn board(args: LaneInspectArgs, config: &AppConfig) -> Result<()> {
    print_clawhip_tmux()?;
    println!();
    print_tmux_panes().await?;

    let roots: Vec<PathBuf> = if !args.worktree_root.is_empty() {
        args.worktree_root
    } else if !config.monitors.lane_worktree_roots.is_empty() {
        config
            .monitors
            .lane_worktree_roots
            .iter()
            .map(PathBuf::from)
            .collect()
    } else {
        println!(
            "(worktree audit skipped: no --worktree-root flag and no lane_worktree_roots configured)"
        );
        Vec::new()
    };

    let active_paths = tmux_pane_cwds().await?;

    for root in roots {
        println!();
        print_worktree_audit(&root, &active_paths, args.limit, false)?;
    }

    Ok(())
}

pub async fn verify(args: LaneVerifyArgs) -> Result<()> {
    let panes = tmux_panes_for_session(&args.session).await?;
    println!("== verify session: {} ==", args.session);
    if panes.is_empty() {
        println!("tmux session/pane not found");
        return Ok(());
    }

    for pane in panes {
        println!();
        println!(
            "-- pane {}:{} pid={} cmd={} dead={} --",
            pane.session,
            pane.pane,
            pane.pid.unwrap_or_default(),
            pane.command.unwrap_or_default(),
            pane.dead
        );
        let Some(cwd) = pane.cwd else {
            println!("cwd: unknown");
            continue;
        };
        println!("cwd: {}", cwd.display());
        let gs = git_status(&cwd);
        if gs.is_git {
            println!("\n[git status]");
            println!("{}", empty_as(&gs.status, "clean"));
            if !gs.diff_stat.is_empty() {
                println!("\n[git diff --stat]");
                println!("{}", gs.diff_stat);
            }
            let pr = gh_pr_for_branch(&cwd);
            if !pr.is_empty() {
                println!("\n[gh pr view current branch]");
                println!("{}", truncate_chars(&pr, 6000));
            }
        } else {
            println!("git: not inside worktree");
        }
        println!("\n[pane tail -{}]", args.lines);
        println!("{}", capture_pane_content(&args.session, args.lines)?);
    }

    Ok(())
}

pub async fn audit_worktrees(args: LaneWorktreesArgs) -> Result<()> {
    let active_paths = tmux_pane_cwds().await?;
    let rows = collect_worktree_rows(&args.path, &active_paths, args.limit)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else {
        print_worktree_rows(&rows);
    }
    Ok(())
}

fn print_clawhip_tmux() -> Result<()> {
    println!("== clawhip tmux ==");
    let output = run(
        &["bash", "-lc", "clawhip tmux list 2>/dev/null || true"],
        None,
    )?;
    println!(
        "{}",
        empty_as(output.stdout.trim(), "No clawhip tmux output")
    );
    Ok(())
}

async fn print_tmux_panes() -> Result<()> {
    println!("== tmux panes ==");
    let panes = tmux_panes_all().await?;
    let mut rows = Vec::new();
    for pane in panes {
        let cwd = pane.cwd.unwrap_or_default();
        let gs = if cwd.as_os_str().is_empty() {
            GitStatus::default()
        } else {
            git_status(&cwd)
        };
        rows.push(vec![
            pane.session,
            pane.pane,
            pane.command.unwrap_or_default(),
            if gs.dirty {
                "dirty".into()
            } else if gs.is_git {
                "clean".into()
            } else {
                "not-git".into()
            },
            gs.dirty_count.to_string(),
            cwd.display().to_string(),
        ]);
    }
    print_table(&["session", "pane", "cmd", "git", "dirty", "cwd"], &rows);
    Ok(())
}

fn print_worktree_audit(
    root: &Path,
    active_paths: &[PathBuf],
    limit: usize,
    quiet: bool,
) -> Result<Vec<Vec<String>>> {
    if !quiet {
        println!("== worktree audit: {} ==", root.display());
    }
    if !root.exists() {
        if !quiet {
            println!("missing");
        }
        return Ok(Vec::new());
    }
    let rows = collect_worktree_rows(root, active_paths, limit)?;
    if !quiet {
        print_worktree_rows(&rows);
    }
    Ok(rows
        .into_iter()
        .map(|row| {
            vec![
                row.state,
                row.dirty.to_string(),
                row.modified.to_string(),
                row.untracked.to_string(),
                row.path,
            ]
        })
        .collect())
}

#[derive(Debug, serde::Serialize)]
struct WorktreeRow {
    state: String,
    dirty: usize,
    #[serde(rename = "mod")]
    modified: usize,
    #[serde(rename = "new")]
    untracked: usize,
    path: String,
}

fn collect_worktree_rows(
    root: &Path,
    active_paths: &[PathBuf],
    limit: usize,
) -> Result<Vec<WorktreeRow>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut dirs = std::fs::read_dir(root)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    dirs.sort();
    let mut rows = Vec::new();
    for path in dirs.into_iter().take(limit) {
        if !is_git_worktree(&path) {
            continue;
        }
        let gs = git_status(&path);
        rows.push(WorktreeRow {
            state: classify_worktree_status(&gs, active_paths),
            dirty: gs.dirty_count,
            modified: gs.modified_count,
            untracked: gs.untracked_count,
            path: path.display().to_string(),
        });
    }
    Ok(rows)
}

fn print_worktree_rows(rows: &[WorktreeRow]) {
    let table_rows = rows
        .iter()
        .map(|row| {
            vec![
                row.state.clone(),
                row.dirty.to_string(),
                row.modified.to_string(),
                row.untracked.to_string(),
                row.path.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["state", "dirty", "mod", "new", "path"], &table_rows);
}

fn classify_worktree_status(gs: &GitStatus, active_paths: &[PathBuf]) -> String {
    let root = &gs.root;
    let active = active_paths.iter().any(|path| {
        !root.as_os_str().is_empty()
            && (root == path || path.starts_with(root) || root.starts_with(path))
    });
    if active {
        "active".into()
    } else if !gs.is_git {
        "not-git".into()
    } else if gs.dirty {
        "dirty-salvage".into()
    } else {
        "clean-removable-if-terminal".into()
    }
}

#[derive(Debug, Default)]
struct GitStatus {
    is_git: bool,
    root: PathBuf,
    status: String,
    diff_stat: String,
    dirty: bool,
    dirty_count: usize,
    modified_count: usize,
    untracked_count: usize,
}

fn git_status(path: &Path) -> GitStatus {
    if !is_git_worktree(path) {
        return GitStatus::default();
    }
    let status = run(
        &[
            "git",
            "-C",
            &path.display().to_string(),
            "status",
            "--short",
            "--branch",
        ],
        None,
    )
    .unwrap_or_default()
    .stdout;
    let diff_stat = run(
        &["git", "-C", &path.display().to_string(), "diff", "--stat"],
        None,
    )
    .unwrap_or_default()
    .stdout;
    let root = run(
        &[
            "git",
            "-C",
            &path.display().to_string(),
            "rev-parse",
            "--show-toplevel",
        ],
        None,
    )
    .ok()
    .filter(|out| out.status.success())
    .map(|out| PathBuf::from(out.stdout.trim()))
    .unwrap_or_else(|| path.to_path_buf());

    let lines = status
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let dirty_lines = lines
        .iter()
        .filter(|line| !line.starts_with("##"))
        .collect::<Vec<_>>();
    let dirty_count = dirty_lines.len();
    let untracked_count = dirty_lines
        .iter()
        .filter(|line| line.starts_with("??"))
        .count();
    let modified_count = dirty_count.saturating_sub(untracked_count);
    GitStatus {
        is_git: true,
        root,
        status,
        diff_stat,
        dirty: dirty_count > 0,
        dirty_count,
        modified_count,
        untracked_count,
    }
}

fn is_git_worktree(path: &Path) -> bool {
    run(
        &[
            "git",
            "-C",
            &path.display().to_string(),
            "rev-parse",
            "--is-inside-work-tree",
        ],
        None,
    )
    .ok()
    .is_some_and(|out| out.status.success() && out.stdout.trim() == "true")
}

#[derive(Debug, Default)]
struct TmuxPaneInfo {
    session: String,
    pane: String,
    pid: Option<String>,
    cwd: Option<PathBuf>,
    command: Option<String>,
    dead: bool,
}

/// Collect pane info for all sessions (replaces the old sync `tmux_panes(None)`).
async fn tmux_panes_all() -> Result<Vec<TmuxPaneInfo>> {
    tmux_panes_raw(None).await
}

/// Collect pane info for a specific session (replaces `tmux_panes(Some(session))`).
async fn tmux_panes_for_session(session: &str) -> Result<Vec<TmuxPaneInfo>> {
    tmux_panes_raw(Some(session)).await
}

/// Collect CWDs from all tmux panes (used for worktree audit).
async fn tmux_pane_cwds() -> Result<Vec<PathBuf>> {
    Ok(tmux_panes_all()
        .await?
        .into_iter()
        .filter_map(|pane| pane.cwd)
        .collect())
}

async fn tmux_panes_raw(session: Option<&str>) -> Result<Vec<TmuxPaneInfo>> {
    let tmux = tmux_bin();
    let mut cmd = tokio::process::Command::new(&tmux);
    cmd.arg("list-panes");
    if let Some(s) = session {
        cmd.arg("-t").arg(s);
    } else {
        cmd.arg("-a");
    }
    cmd.arg("-F").arg(
        "#{session_name}\t#{window_index}.#{pane_index}\t#{pane_pid}\t#{pane_current_path}\t#{pane_current_command}\t#{pane_active}\t#{pane_dead}",
    );

    let output = cmd.output().await?;
    if !output.status.success() {
        return Ok(Vec::new());
    }

    let panes = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() < 7 {
                return None;
            }
            Some(TmuxPaneInfo {
                session: parts[0].to_string(),
                pane: parts[1].to_string(),
                pid: Some(parts[2].to_string()).filter(|s| !s.is_empty()),
                cwd: Some(PathBuf::from(parts[3])).filter(|p| !p.as_os_str().is_empty()),
                command: Some(parts[4].to_string()).filter(|s| !s.is_empty()),
                dead: parts[6] == "1",
            })
        })
        .collect();
    Ok(panes)
}

fn capture_pane_content(session: &str, lines: usize) -> Result<String> {
    let start = format!("-{lines}");
    let output = run(
        &["tmux", "capture-pane", "-pt", session, "-S", &start],
        None,
    )?;
    Ok(if output.status.success() {
        output.stdout
    } else {
        output.stderr
    })
}

fn gh_pr_for_branch(path: &Path) -> String {
    let branch = run(
        &[
            "git",
            "-C",
            &path.display().to_string(),
            "branch",
            "--show-current",
        ],
        None,
    )
    .ok()
    .filter(|out| out.status.success())
    .map(|out| out.stdout.trim().to_string())
    .unwrap_or_default();
    if branch.is_empty() {
        return String::new();
    }
    run(
        &[
            "gh",
            "pr",
            "view",
            &branch,
            "--json",
            "number,url,state,isDraft,mergeStateStatus,headRefOid,baseRefName,statusCheckRollup",
            "--jq",
            ".",
        ],
        Some(path),
    )
    .ok()
    .filter(|out| out.status.success())
    .map(|out| out.stdout)
    .unwrap_or_default()
}

#[derive(Default)]
struct CmdOutput {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}

fn run(args: &[&str], cwd: Option<&Path>) -> Result<CmdOutput> {
    let mut command = Command::new(args[0]);
    command.args(&args[1..]);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    let output = command.output()?;
    Ok(CmdOutput {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    if rows.is_empty() {
        println!("(none)");
        return;
    }
    let mut widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(cell.replace('\n', " ").chars().count().min(120));
        }
    }
    println!(
        "{}",
        headers
            .iter()
            .enumerate()
            .map(|(idx, h)| format!("{h:<width$}", width = widths[idx]))
            .collect::<Vec<_>>()
            .join("  ")
    );
    println!(
        "{}",
        widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(idx, cell)| {
                    let cell = truncate_chars(&cell.replace('\n', " "), 120);
                    format!("{cell:<width$}", width = widths[idx])
                })
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

fn empty_as<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

fn truncate_chars(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}
