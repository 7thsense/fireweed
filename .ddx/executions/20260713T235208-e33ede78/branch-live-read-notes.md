# Branch live-read conclusion

- `pqueue-c33c367e` interaction stays compatible with the trimmed-source branch fix.
- Branch creation now copies retained source segments into branch-owned `branch-seg/` objects.
- After source manifest/segment prefixes are physically deleted, branch reads continue from the branch live view and do not GET deleted source objects.
- No queue semantics, retention-floor rules, or atomicity guarantees were changed.
