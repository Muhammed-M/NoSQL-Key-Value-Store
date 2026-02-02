# GitHub Setup Instructions

The repository has been initialized and committed locally. Follow these steps to push to GitHub:

## 1. Create a GitHub Repository

1. Go to https://github.com and log in
2. Click the "+" icon in the top right corner
3. Select "New repository"
4. Name it (e.g., "nosql-kv-store")
5. **Do NOT** initialize with README, .gitignore, or license (we already have these)
6. Click "Create repository"

## 2. Add Remote and Push

After creating the repository, GitHub will show you commands. Use these:

```bash
# Add the remote (replace YOUR_USERNAME and REPO_NAME)
git remote add origin https://github.com/YOUR_USERNAME/REPO_NAME.git
git remote add origin https://github.com/Muhammed-M/NoSQL-Key-Value-Store.git
git branch -M main
git push -u origin main
# Push to GitHub
git branch -M main
git push -u origin main
```

Or if you prefer SSH:

```bash
git remote add origin git@github.com:YOUR_USERNAME/REPO_NAME.git
git branch -M main
git push -u origin main
```

## 3. Share the Link

Once pushed, share the repository link in your Telegram chat.

## Quick Commands Reference

```bash
# Check status
git status

# View commits
git log

# Push updates
git add .
git commit -m "Your commit message"
git push
```

