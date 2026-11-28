// The hash of the code a test report or crash matrix was produced from (S3-F9). sha256 over the sorted list of
// (path, content) of every tracked or untracked-but-not-ignored file under SOURCE_DIRS, minus SOURCE_EXCLUDE.
// Same value locally (before the commit) and in CI (after it), as long as every file is committed.
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

export const SOURCE_DIRS = ['packages', 'services', 'tests', 'tools', 'db', 'schemas'];
/** Docs, and this checker itself (changing it does not change any test result). */
export const SOURCE_EXCLUDE = [/\.md$/i, /^tools\/guarantee-check\//];

export function sourceFiles(repo: string): string[] {
  const out = execFileSync('git', ['ls-files', '-co', '--exclude-standard', '--', ...SOURCE_DIRS], { cwd: repo, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  return out
    .split('\n')
    .filter(Boolean)
    .filter((p) => !SOURCE_EXCLUDE.some((re) => re.test(p)))
    .sort();
}

export function hashFiles(entries: { path: string; content: Buffer | string }[]): string {
  const h = createHash('sha256');
  for (const e of [...entries].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))) {
    h.update(e.path);
    h.update('\0');
    h.update(e.content);
    h.update('\0');
  }
  return h.digest('hex');
}

export function sourceHash(repo: string): string {
  return hashFiles(
    sourceFiles(repo).flatMap((path) => {
      try {
        return [{ path, content: readFileSync(resolve(repo, path)) }];
      } catch {
        return []; // listed but deleted in the working tree
      }
    }),
  );
}

export const fileHash = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex');

export function gitCommit(repo: string): { commit: string; dirty: boolean } {
  try {
    const commit = execFileSync('git', ['rev-parse', '--short', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim();
    const dirty = execFileSync('git', ['status', '--porcelain'], { cwd: repo, encoding: 'utf8' }).trim() !== '';
    return { commit, dirty };
  } catch {
    return { commit: 'unknown', dirty: false };
  }
}
