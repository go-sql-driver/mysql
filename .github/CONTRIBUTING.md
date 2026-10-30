# Contributing Guidelines

## Reporting Issues

Before creating a new Issue, please check first if a similar Issue [already exists](https://github.com/go-sql-driver/mysql/issues?state=open) or was [recently closed](https://github.com/go-sql-driver/mysql/issues?direction=desc&page=1&sort=updated&state=closed).

## Contributing Code

By contributing to this project, you share your code under the Mozilla Public License 2, as specified in the LICENSE file.
Don't forget to add yourself to the AUTHORS file.

### Code Review

Everyone is invited to review and comment on pull requests.
If it looks fine to you, comment with "LGTM" (Looks good to me).

If changes are required, notice the reviewers with "PTAL" (Please take another look) after committing the fixes.

Before merging the Pull Request, at least one [team member](https://github.com/go-sql-driver?tab=members) must have commented with "LGTM".

## Development Ideas

If you are looking for ideas for code contributions, please check our [Development Ideas](https://github.com/go-sql-driver/mysql/wiki/Development-Ideas) Wiki page.

## GitHub Actions dependencies

Action versions and their transitive dependencies are pinned in
`.github/workflows/actions.lock`. After adding a workflow or changing a `uses:`
reference, regenerate and verify the lockfile with the official GitHub CLI extension:

```sh
gh extension install github/gh-actions-lock --pin v0.4.0
gh actions-lock
gh actions-lock --verify
```

Review and commit the workflow and lockfile changes together. Do not edit the
lockfile manually. Dependabot updates existing workflow dependencies and their
lockfile entries together.
