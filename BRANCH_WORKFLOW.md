# Git Branch Workflow Guide

## How to Create and Work with Private Branches

### 1. Create a New Branch
```bash
# Create and switch to a new branch for your feature/fix
git checkout -b feature/your-feature-name
# or for fixes
git checkout -b fix/your-fix-name
```

### 2. Work on Your Branch
```bash
# Make your changes...
# Add files as needed
git add file1.go file2.go

# Commit your changes with descriptive messages
git commit -m "Add feature X with proper error handling"
```

### 3. Push Your Branch to Remote
```bash
# Push the branch to GitHub (first time)
git push -u origin feature/your-feature-name

# For subsequent pushes
git push
```

### 4. Create a Pull Request
- Visit the URL shown after pushing (e.g., `https://github.com/ljluestc/mysql/pull/new/feature/your-feature-name`)
- Or go to GitHub and click "Compare & pull request"
- Fill in the PR description and submit

### 5. Switch Between Branches
```bash
# Switch back to master
git checkout master

# Switch to your feature branch
git checkout feature/your-feature-name

# See all branches
git branch -v
```

### 6. Merge Your Branch (After PR Approval)
```bash
# Switch to master
git checkout master

# Pull latest changes
git pull origin master

# Merge your branch
git merge feature/your-feature-name

# Push the merge
git push origin master

# Delete the local branch (optional)
git branch -d feature/your-feature-name
```

### 7. Clean Up
```bash
# Delete remote branch after merge
git push origin --delete feature/your-feature-name

# Delete local branch
git branch -d feature/your-feature-name
```

## Branch Naming Conventions

- **Features**: `feature/description-of-feature`
- **Fixes**: `fix/description-of-fix`
- **Documentation**: `docs/update-documentation`
- **Tests**: `test/add-tests-for-feature`

## Best Practices

1. **Keep branches focused** - One feature or fix per branch
2. **Commit often** - Small, logical commits with clear messages
3. **Pull master regularly** - Keep your branch up to date
4. **Write good PR descriptions** - Explain what you changed and why
5. **Delete merged branches** - Keep the repository clean

## Current Branch Status

You now have:
- **master**: Main branch with your initial commit
- **fix/transaction-id-documentation**: Branch with documentation files

The documentation branch is ready for a pull request at:
https://github.com/ljluestc/mysql/pull/new/fix/transaction-id-documentation
