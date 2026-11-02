import os, shutil, subprocess


class DirRemote:
    """Folder-backed storage: used by tests and for local runs (DATA_DIR)."""
    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def fetch(self, name, dest_dir):
        src = os.path.join(self.root, name)
        if not os.path.exists(src):
            return None
        os.makedirs(dest_dir, exist_ok=True)
        dst = os.path.join(dest_dir, name)
        if os.path.abspath(src) != os.path.abspath(dst):
            shutil.copyfile(src, dst)
        return dst

    def push(self, path):
        dst = os.path.join(self.root, os.path.basename(path))
        if os.path.abspath(path) != os.path.abspath(dst):
            shutil.copyfile(path, dst)

    def names(self):
        return sorted(os.listdir(self.root))


class ReleaseRemote:
    """GitHub Release assets as an object store (tag 'data'). Needs `gh` + GH_TOKEN."""
    def __init__(self, repo, tag="data"):
        self.repo, self.tag = repo, tag
        if self._gh("release", "view", tag).returncode != 0:
            r = self._gh("release", "create", tag, "--title", "market data",
                         "--notes", "Parquet data lake (managed by ops workflow)")
            if r.returncode != 0:
                raise RuntimeError("cannot create release: " + r.stderr[:200])

    def _gh(self, *args):
        return subprocess.run(["gh", *args, "-R", self.repo], capture_output=True, text=True)

    def fetch(self, name, dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
        r = self._gh("release", "download", self.tag, "-p", name, "-D", dest_dir, "--clobber")
        path = os.path.join(dest_dir, name)
        return path if r.returncode == 0 and os.path.exists(path) else None

    def push(self, path):
        r = self._gh("release", "upload", self.tag, path, "--clobber")
        if r.returncode != 0:
            raise RuntimeError(f"upload failed for {path}: {r.stderr[:200]}")


def from_env():
    if os.environ.get("DATA_DIR"):
        return DirRemote(os.environ["DATA_DIR"])
    return ReleaseRemote(os.environ["GITHUB_REPOSITORY"])
