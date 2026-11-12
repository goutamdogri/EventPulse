# Notebook learing

Command to generate folder tree structure that are tracked by the git

```bash
git ls-files | tree --fromfile > current-project-structure.txt
```

## Nest js

### To add a package

```bash
cd services
pnpm dlx @nestjs/cli@latest new ingest-api --package-manager pnpm --strict --skip-git
cd ingest-api
rm -rf node_modules pnpm-lock.yaml     # the root workspace owns the lockfile 

# we want a single workspace-managed dependency installation.
cd ../..            # repo root
pnpm install
pnpm --filter @eventpulse/ingest-api add <dependency_name>
pnpm --filter @eventpulse/ingest-api add -D <dependency_name>
pnpm --filter @eventpulse/ingest-api remove <dependency_name>
```
