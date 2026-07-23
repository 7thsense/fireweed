---
ddx:
  id: adr-021-open-source-license-and-contribution-policy
  depends_on:
    - adr-020-public-namespace-and-compatibility
  links:
    - {kind: informed_by, to: adr-020-public-namespace-and-compatibility}
  status: accepted
---

# ADR-021: Open-source license and contribution policy for Fireweed Queue

| Date | Status | Deciders | Related |
|------|--------|----------|---------|
| 2026-07-23 | Accepted | Project maintainers | ADR-020, public-namespace-migration |

## Context

Fireweed Queue is being published as a maintainer-developed open-source project.
The repository metadata already selects the dual-license expression
`MIT OR Apache-2.0`, and the public preview boundary treats the project as an
externally named product with a controlled support posture.

The project needs a clear outbound license and a contribution policy that match
the current operating model:

- maintainers accept issues for bugs, feature requests, documentation problems,
  and compatibility reports;
- maintainers do not accept pull requests or other code contributions;
- the GitHub pull-request feature will be disabled at public cutover;
- no CLA or DCO is required while code contributions remain closed;
- security reports must use the private security-reporting route, not public
  issues;
- generic contributor notices must use the collective Fireweed Queue wording
  without trying to split personal and company ownership.

This ADR records the policy. It does not make external GitHub setting changes
itself.

## Decision

Adopt `MIT OR Apache-2.0` as the outbound license for Fireweed Queue and keep
the project in an issues-only contribution posture.

Decision owner: Project maintainers.
Decision date: 2026-07-23.

The governance rules are:

- issues are welcome for bugs, feature requests, documentation problems, and
  compatibility reports;
- pull requests are not accepted, and other code contributions are not
  accepted;
- at public cutover, the GitHub pull-request feature will be disabled so the
  public surface matches the policy;
- no CLA or DCO applies while code contributions remain closed;
- this policy is maintained as a maintainer-developed open-source project
  decision, not a community-contribution program.

## License Terms

The outbound license is `MIT OR Apache-2.0`.

That means:

- recipients may use either license path under the dual-license grant;
- Apache-2.0 patent terms apply when Apache-2.0 is chosen, including the patent
  license and termination conditions of that license;
- the repository metadata and release artifacts must keep the dual-license
  expression aligned with this ADR.

## Contribution Policy

The project accepts issues, not code submissions.

Accepted issue content includes:

- bug reports;
- feature requests;
- documentation problems;
- compatibility reports.

Not accepted:

- pull requests;
- patch files submitted as code contributions;
- branch-based or fork-based code changes offered as the contribution channel.

This is a deliberate policy choice for the public preview and cutover phases.
It is not a temporary omission and it does not imply that the project is open
to code review as a substitute contribution path.

## Inbound Licensing For Issues

Small original snippets or authorized reproduction snippets intentionally
submitted through issues are offered under `MIT OR Apache-2.0` so maintainers
can quote, test, or adapt them without a separate rights chase.

That inbound grant applies only to material intentionally submitted in the issue
channel and only to the extent needed to use that material in the project.
Submitters should keep snippets small and should not use issues as a substitute
for code contribution.

## Third-Party Material Review

Issues may include third-party material, but maintainers must review provenance
before reusing it.

Policy:

- contributors must identify any third-party source material they include in an
  issue when the provenance is not obvious;
- maintainers must not assume that copied text, code, tables, or examples are
  original to the reporter;
- material with unclear provenance is not safe to reuse until it is reviewed
  against the claimed source license and attribution requirements;
- if a snippet is based on a third-party source, the issue should say so
  explicitly.

## Security Reporting

Security reports do not belong in public issues.

They must use `SECURITY.md` and the private security-reporting path described
there. Public issues remain the channel for ordinary bugs, documentation, and
compatibility reports, but not for vulnerabilities or exploit disclosure.

## NOTICE Requirements

If packaging, redistribution, or a downstream dependency requires a NOTICE file
or equivalent notices section, maintainers must include the required third-party
notices and attribution text.

The notice text should use the collective label `Fireweed Queue contributors`
for generic contributor notices. Do not split that wording into personal versus
company ownership unless a specific legal notice requires it.

## Consequences

The project keeps a simple public contract:

- the outbound license is permissive and widely compatible;
- community reporting stays open through issues;
- code acceptance stays closed until the project intentionally revises this ADR;
- security handling stays off the public issue tracker;
- future cutover work can disable the GitHub pull-request feature without
  changing the decision recorded here.

The tradeoff is that external code contributions are not part of the public
collaboration model, so maintainers need to keep issue triage responsive and
documented.
