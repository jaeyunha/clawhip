pub const DEFAULT_KEYWORD_WINDOW_SECS: u64 = 30;

/// Serde default helper — returns [`DEFAULT_KEYWORD_WINDOW_SECS`].
pub fn default_keyword_window_secs() -> u64 {
    DEFAULT_KEYWORD_WINDOW_SECS
}

/// Conservative POSIX single-quote escaper shared across the codebase.
///
/// Leaves a string unquoted only when every character is alphanumeric or in
/// `_@%+=:,./-` (the union-safe set). Otherwise wraps in single-quotes using
/// the `'\''` idiom to embed literal single-quotes.
pub fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || "_@%+=:,./-".contains(ch))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_strings_are_unquoted() {
        assert_eq!(shell_quote("hello"), "hello");
        assert_eq!(shell_quote("foo-bar"), "foo-bar");
        assert_eq!(shell_quote("path/to/file.txt"), "path/to/file.txt");
        assert_eq!(shell_quote("user@host"), "user@host");
        assert_eq!(shell_quote("key=value"), "key=value");
        assert_eq!(shell_quote("100%"), "100%");
        assert_eq!(shell_quote("a+b"), "a+b");
        assert_eq!(shell_quote("a,b"), "a,b");
        assert_eq!(shell_quote("a:b"), "a:b");
        assert_eq!(shell_quote("_priv"), "_priv");
    }

    #[test]
    fn unsafe_strings_are_single_quoted() {
        // Hyphen-minus used in shell: no issue, but spaces need quoting
        assert_eq!(shell_quote("hello world"), "'hello world'");
        assert_eq!(shell_quote("<@123>"), "'<@123>'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("$HOME"), "'$HOME'");
        assert_eq!(shell_quote("a b"), "'a b'");
    }

    #[test]
    fn ledger_shell_quote_compat_alphanumeric_and_dash() {
        // Values that the old ledger::shell_quote allowed unquoted
        assert_eq!(shell_quote("agent-1"), "agent-1");
        assert_eq!(shell_quote("alerts"), "alerts");
        assert_eq!(
            shell_quote("READY_FOR_REVIEW,BLOCKED"),
            "READY_FOR_REVIEW,BLOCKED"
        );
        // old ledger did NOT allow @ unquoted; new shared fn does (union-safe)
        assert_eq!(shell_quote("<@123>"), "'<@123>'");
    }

    #[test]
    fn tmux_shell_escape_compat() {
        // Values the old tmux_wrapper::shell_escape allowed unquoted (it also
        // allowed @, %, +, =, :, comma, dot, slash, dash, underscore — all
        // covered by the shared set).
        assert_eq!(shell_quote("tmux-session"), "tmux-session");
        assert_eq!(shell_quote("key=value"), "key=value");
        assert_eq!(shell_quote("path/to"), "path/to");
    }
}
