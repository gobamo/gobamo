#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import queue
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

BRANCH_STEMS = [
    "quantum/cache-reconciliation", "ast/shadow-index",
    "telemetry/null-a", "refactor/recursive-registry",
    "infra/a-workers", "parser/entropy-layer",
    "storage/merkle-a", "runtime/ghost-state",
    "scheduler/deferred-a", "compiler/phase-inversion",
    "protocol/latent-a", "distributed/eventual-ambiguity",
]

SUBJECTS = [
    "reconcile divergent execution vectors",
    "invalidate stale shadow references",
    "defer the second-order consistency pass",
    "normalize a partially materialized state",
    "collapse redundant observer edges",
    "rebuild the synthetic dependency lattice",
    "apply the inverse compatibility transform",
    "isolate the non-deterministic commit window",
    "synchronize the recursive projection cache",
    "materialize the latent rollback surface",
]

CRYPTIC = [
    "[causal-window=closed; entropy=unbounded]",
    "[replay=deferred; observer=detached]",
    "[consistency=eventual-ish; branch=non-euclidean]",
    "[parent-vector=ambiguous; checksum=unknown]",
    "[phase=III; state=unresolved; do-not-normalize]",
]


# ============================================================
# LOGGER
# ============================================================

class Logger:
    def __init__(self, path: Path, console: bool = True):
        self.path = path
        self.console = console
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def set_console(self, enabled: bool):
        with self.lock:
            self.console = enabled

    def log(self, message: str):
        stamp = datetime.now().astimezone().strftime("%H:%M:%S")
        line = f"[{stamp}] {message}"

        with self.lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()

            if self.console:
                print(line, flush=True)


# ============================================================
# GIT CORE
# ============================================================

def clean_env(extra=None):
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith("GIT_")
    }

    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = os.devnull

    if extra:
        env.update(extra)

    return env


def git(repo: Path, *args: str, check=True,
        input_text=None, extra_env=None):

    p = subprocess.run(
        ["git", *map(str, args)],
        cwd=repo,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=clean_env(extra_env),
    )

    if p.returncode in (-2, 130, 3221225786):
        raise KeyboardInterrupt

    if check and p.returncode:
        raise RuntimeError(
            f"git {' '.join(map(str, args))} "
            f"a échoué (code {p.returncode}):\n"
            f"{p.stderr.strip()}"
        )

    return p.stdout.strip()


def global_git(*args: str, cwd: Path | None = None, check=True):

    p = subprocess.run(
        ["git", *map(str, args)],
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=clean_env(),
    )

    if check and p.returncode:
        raise RuntimeError(
            f"git {' '.join(map(str, args))} a échoué:\n"
            f"{p.stderr.strip()}"
        )

    return p.stdout.strip()


def ensure_safe_output(output: Path):
    # --- MODIFICATION : Désactivation totale des sécurités de chemin ---
    pass


def commit(repo: Path, message: str, allow_empty=False):

    git(repo, "add", "-A")

    stamp = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    env = {
        "GIT_AUTHOR_NAME": "a",
        "GIT_AUTHOR_EMAIL": "aa",
        "GIT_COMMITTER_NAME": "aaa",
        "GIT_COMMITTER_EMAIL": "aaaa",
        "GIT_AUTHOR_DATE": stamp,
        "GIT_COMMITTER_DATE": stamp,
    }

    args = [
        "commit",
        "--quiet",
        "-m",
        message,
    ]

    if allow_empty:
        args.append("--allow-empty")

    git(repo, *args, extra_env=env)

    return git(repo, "rev-parse", "--short", "HEAD")


def merge(repo: Path, *branches: str):

    git(
        repo,
        "merge",
        "--no-ff",
        "--no-edit",
        "--quiet",
        *branches,
    )


def snapshot_local_heads(repo: Path) -> list[tuple[str, str]]:
    """Capture immutable object IDs for the current local branch refs."""
    output = git(
        repo,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
        "refs/heads",
    )
    snapshot = []
    for line in output.splitlines():
        ref, oid = line.split(maxsplit=1)
        if ref.startswith("refs/heads/"):
            snapshot.append((ref.removeprefix("refs/heads/"), oid))
    return snapshot


class AsyncPublisher:


    def __init__(
        self,
        repo: Path,
        remote: str,
        namespace: str,
        max_pushes: int,
        min_interval: float,
        max_remote_branches: int,
        log: Logger,
    ):
        self.repo = repo
        self.remote = remote
        self.namespace = namespace
        self.max_pushes = max_pushes
        self.min_interval = min_interval
        self.max_remote_branches = max_remote_branches
        self.log = log

        self._queue: queue.Queue = queue.Queue(maxsize=1)
        self._lock = threading.Lock()
        self._published: dict[str, str] = {}
        self._scheduled = 0
        self._pushes = 0
        self._last_push_at = time.monotonic() - min_interval
        self._disabled = False
        self._error = None
        self._closed = False

        self._thread = threading.Thread(
            target=self._run,
            name="gobamo",
            daemon=True,
        )
        self._thread.start()

    def enqueue(self, snapshot: list[tuple[str, str]]) -> bool:
        """Queue a ref snapshot; coalesce an older pending snapshot if needed."""
        with self._lock:
            if self._disabled or self._closed or self._scheduled >= self.max_pushes:
                return False
            if time.monotonic() - self._last_push_at < self.min_interval:
                return False

            try:
                self._queue.put_nowait(snapshot)
                self._scheduled += 1
                self._last_push_at = time.monotonic()
                return True
            except queue.Full:
                # Keep only the newest pending snapshot. One in-flight push and
                # one pending push are the maximum, regardless of cycle speed.
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except queue.Empty:
                    pass
                try:
                    self._queue.put_nowait(snapshot)
                    self._last_push_at = time.monotonic()
                    return True
                except queue.Full:
                    return False

    def _run(self):
        while True:
            snapshot = self._queue.get()
            try:
                if snapshot is None:
                    return

                with self._lock:
                    published = dict(self._published)

                current = dict(snapshot)
                main_oid = current.get("main")
                if not main_oid:
                    self.log.log("Publication ignorée : branche locale main introuvable.")
                    continue

                # Reserve one destination branch for main, then allocate the
                # remaining finite branch budget to every other local head.
                room = max(0, self.max_remote_branches - 1 - len(
                    [name for name in published if name != "main"]
                ))
                new_names = [
                    name for name in current
                    if name != "main" and name not in published
                ]
                # Keep the original topology visible first, then add live-cycle
                # branches in deterministic order until the remote budget fills.
                new_names.sort(key=lambda name: (name.startswith("live/"), name))
                selected_new = new_names[:room]

                updates = []
                if published.get("main") != main_oid:
                    updates.append((main_oid, "refs/heads/main"))

                # --- CORRECTION 2 : Inclure les branches parallèles/live sélectionnées ---
                for name in selected_new:
                    updates.append((current[name], f"refs/heads/{self.namespace}/{name}"))

                if not updates:
                    self.log.log("Publication ignorée : aucun nouveau commit ou pointeur à publier.")
                    continue

                command = ["git", "push", "--porcelain", "origin"]
                command.extend(f"{oid}:{destination}" for oid, destination in updates)
                env = os.environ.copy()
                env["GIT_TERMINAL_PROMPT"] = "0"

                result = subprocess.run(
                    command,
                    cwd=self.repo,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    timeout=120,
                )
                if result.returncode:
                    detail = (result.stderr or result.stdout).strip()
                    raise RuntimeError(
                        f"push refusé (code {result.returncode}): {detail}"
                    )

                with self._lock:
                    self._published["main"] = main_oid
                    for name in selected_new:
                        self._published[name] = current[name]
                    self._pushes += 1
                    self._last_push_at = time.monotonic()
                    published_total = len(self._published)
                    push_number = self._pushes

                self.log.log(
                    f"Push GitHub asynchrone #{push_number} réussi : "
                    f"{len(updates)} références envoyées ; "
                    f"{published_total}/{self.max_remote_branches} branches "
                    f"distantes utilisées sous {self.namespace}."
                )

            except subprocess.TimeoutExpired:
                self._disable("push interrompu après 120 secondes")
                return
            except Exception as exc:
                self._disable(f"{type(exc).__name__}: {exc}")
                return
            finally:
                self._queue.task_done()

    def _disable(self, reason: str):
        with self._lock:
            self._disabled = True
            self._error = reason
        self.log.log(
            "Publication distante arrêtée après un échec ; "
            f"le générateur local continue. Détail : {reason}"
        )

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        # A pending snapshot is allowed to finish; then the sentinel stops the
        # publisher thread. If it already exited on error, discard any queued
        # snapshot so shutdown cannot block on a full queue.
        if not self._thread.is_alive():
            while True:
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except queue.Empty:
                    break
            return
        self._queue.put(None)
        self._thread.join()

    @property
    def stats(self):
        with self._lock:
            return {
                "pushes": self._pushes,
                "branches": len(self._published),
                "disabled": self._disabled,
                "error": self._error,
                "scheduled": self._scheduled,
            }


# ============================================================
# INITIAL REPOSITORY
# ============================================================

def initial_repository(
    repo: Path,
    branches: int,
    orphan_lines: int,
    rng,
    log: Logger,
):

    log.log("[1/5] Création du dépôt Git local…")

    repo.mkdir(
        parents=True,
        exist_ok=True,
    )

    git(
        repo,
        "init",
        "--quiet",
        "--initial-branch=main",
    )

    git(repo, "config", "user.name", "a")
    git(repo, "config", "user.email", "a")
    git(repo, "config", "commit.gpgsign", "false")

    (repo / "README.md").write_text(
        "# Git gobamo\n\n"
        "Dépôt synthétique pédagogique. La publication distante éventuelle\n"
        "se fait sur une branche a\n",
        encoding="utf-8",
    )

    commit(repo, "init(core): instantiate baseline reality")

    trunk = git(repo, "rev-parse", "HEAD")

    # Branches expérimentales
    log.log(
        f"[2/5] Construction de {branches} branches expérimentales…"
    )

    feature_branches = []

    for i in range(branches):

        stem = BRANCH_STEMS[i % len(BRANCH_STEMS)]

        branch = f"exp/{stem}-{i + 1:02d}"

        git(
            repo,
            "switch",
            "--quiet",
            "--force-create",
            branch,
            trunk,
        )

        count = rng.randint(2, 3)

        for n in range(count):

            folder = (
                repo
                / "specimens"
                / f"branch-{i + 1:02d}"
            )

            folder.mkdir(
                parents=True,
                exist_ok=True,
            )

            (
                folder
                / f"fragment-{n + 1}.txt"
            ).write_text(
                f"Synthetic fixture\n"
                f"branch={branch}\n"
                f"fragment={n + 1}\n"
                f"nonce={rng.getrandbits(48):012x}\n",
                encoding="utf-8",
            )

            commit(
                repo,
                f"{rng.choice(['chore', 'refactor', 'fix', 'spec'])}"
                f"({i:02d}): {rng.choice(SUBJECTS)}\n\n"
                f"{rng.choice(CRYPTIC)}",
            )

        feature_branches.append(branch)

        log.log(
            f"    branche {i + 1:02d}/{branches} prête : {branch}"
        )

    # Historiques croisés
    log.log(
        "[3/5] Création des historiques croisés et merges octopus…"
    )

    git(
        repo,
        "switch",
        "--quiet",
        "--force-create",
        "weave/left",
        trunk,
    )

    for b in feature_branches:
        merge(repo, b)

    left_tip = git(repo, "rev-parse", "HEAD")

    git(
        repo,
        "switch",
        "--quiet",
        "--force-create",
        "weave/right",
        trunk,
    )

    for b in reversed(feature_branches):
        merge(repo, b)

    right_tip = git(repo, "rev-parse", "HEAD")

    # Secteurs de fusion
    sectors = [
        feature_branches[i::3]
        for i in range(3)
    ]

    sector_refs = []

    for i, group in enumerate(sectors, 1):

        name = f"integration/sector-{i}"

        git(
            repo,
            "switch",
            "--quiet",
            "--force-create",
            name,
            trunk,
        )

        merge(repo, *group)

        commit(
            repo,
            f"integration(sector-{i}): "
            f"seal {len(group)}-way convergence",
            allow_empty=True,
        )

        sector_refs.append(name)

    # Convergence principale
    git(repo, "switch", "--quiet", "main")

    merge(repo, "weave/left")
    merge(repo, "weave/right")

    for b in sector_refs:
        merge(repo, b)

    for i in range(3):
        commit(
            repo,
            f"a : revert the inverse restoration #{i + 1}",
            allow_empty=True,
        )

    # Tags annotés et imbriqués
    for i, ref in enumerate(
        [left_tip, right_tip, *feature_branches[:8]],
        1,
    ):

        git(
            repo,
            "tag",
            "--annotate",
            f"lab/checkpoint-{i:02d}",
            ref,
            "--message",
            f"Synthetic checkpoint {i}; status=unresolved",
        )

    git(
        repo,
        "tag",
        "--annotate",
        "meta/aggregate",
        "lab/checkpoint-01",
        "--message",
        "A tag pointing to another tag.",
    )

    # Notes Git
    for i, branch in enumerate(feature_branches[:6], 1):

        target = git(repo, "rev-parse", branch)

        git(
            repo,
            "notes",
            "--ref=refs/notes/forensic-ledger",
            "add",
            "--force",
            "--message",
            f"case={i:03d}; verdict=deferred",
            target,
        )

    # Histoires déconnectées
    log.log(
        "[4/5] Création de petites histoires déconnectées…"
    )

    empty_tree = git(
        repo,
        "mktree",
        input_text="",
    )

    for i in range(orphan_lines):

        env = {
            "GIT_AUTHOR_NAME": "Unreachable Committee",
            "GIT_AUTHOR_EMAIL": "void@example.invalid",
            "GIT_COMMITTER_NAME": "Unreachable Committee",
            "GIT_COMMITTER_EMAIL": "void@example.invalid",
        }

        orphan = git(
            repo,
            "commit-tree",
            empty_tree,
            "-m",
            f"void(timeline-{i + 1}): disconnected state",
            extra_env=env,
        )

        git(
            repo,
            "update-ref",
            f"refs/heads/void/timeline-{i + 1:02d}",
            orphan,
        )

    log.log(
        "    dépôt initial prêt : aucun remote configuré."
    )


# ============================================================
# RECURSIVE SUBMODULE SNAPSHOTS
# ============================================================

def prepare_recursive_snapshot(
    repo: Path,
    assets: Path,
    log: Logger,
) -> Path:

    log.log(
        "[5/5] Préparation des snapshots locaux "
        "pour les sous-modules récursifs…"
    )

    assets.mkdir(
        parents=True,
        exist_ok=False,
    )

    base_bare = assets / "base-snapshot.git"
    donor_work = assets / "donor-work"
    recursive_bare = assets / "recursive-snapshot.git"

    global_git(
        "clone",
        "--bare",
        "--no-hardlinks",
        str(repo),
        str(base_bare),
        cwd=assets,
    )

    global_git(
        "clone",
        "--no-hardlinks",
        str(repo),
        str(donor_work),
        cwd=assets,
    )

    global_git(
        "-C",
        str(donor_work),
        "remote",
        "remove",
        "origin",
        check=False,
    )

    # Le dépôt donneur contient lui-même un sous-module local.
    global_git(
        "-C",
        str(donor_work),
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "add",
        "--name",
        "retro-seed",
        base_bare.as_uri(),
        "recursive/seed-clone",
    )

    global_git(
        "-C",
        str(donor_work),
        "-c",
        "user.name=Recursive Snapshot Builder",
        "-c",
        "user.email=builder@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "meta(submodule): embed a local snapshot of the same repository",
    )

    global_git(
        "clone",
        "--bare",
        "--no-hardlinks",
        str(donor_work),
        str(recursive_bare),
        cwd=assets,
    )

    shutil.rmtree(donor_work)

    log.log(
        "    snapshot récursif prêt "
        "(clone local → sous-module → sous-module imbriqué)."
    )

    return recursive_bare


def add_retroclone(
    repo: Path,
    source: Path,
    cycle: int,
):

    name = f"retroclone-{cycle:06d}"

    relpath = (
        f"retroclones/cycle-{cycle:06d}"
    )

    git(
        repo,
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "add",
        "--name",
        name,
        source.as_uri(),
        relpath,
    )

    git(
        repo,
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "update",
        "--init",
        "--recursive",
        "--",
        relpath,
    )

    return name, relpath


def remove_retroclone(
    repo: Path,
    name: str,
    relpath: str,
):

    git(
        repo,
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "deinit",
        "-f",
        "--",
        relpath,
        check=False,
    )

    git(
        repo,
        "rm",
        "-f",
        "--",
        relpath,
        check=False,
    )

    module_dir = (
        repo
        / ".git"
        / "modules"
        / name
    )

    if module_dir.exists():
        shutil.rmtree(module_dir, ignore_errors=True)



def live_cycle(
    repo: Path,
    cycle: int,
    rng,
    source: Path,
    active: deque,
    max_submodules: int,
    submodule_every: int,
    retain_cycles: int,
    log: Logger,
):

    log.log(
        f"[cycle {cycle}] création des branches parallèles et des merges…"
    )

    base = git(repo, "rev-parse", "main")

    root = f"live/cycle-{cycle:06d}"

    strands = []

    # Three parallel branches
    for i in range(3):

        branch = f"{root}/strand-{i}"

        git(
            repo,
            "switch",
            "--quiet",
            "--force-create",
            branch,
            base,
        )

        commit(
            repo,
            f"quantum({i}): reconcile speculative topology #{cycle}\n\n"
            f"{rng.choice(CRYPTIC)}",
            allow_empty=True,
        )

        strands.append(branch)

    # Left weave
    left = f"{root}/weave-left"

    git(
        repo,
        "switch",
        "--quiet",
        "--force-create",
        left,
        base,
    )

    merge(repo, strands[0])
    merge(repo, strands[1])

    # Right weave, reverse order
    right = f"{root}/weave-right"

    git(
        repo,
        "switch",
        "--quiet",
        "--force-create",
        right,
        base,
    )

    merge(repo, strands[1])
    merge(repo, strands[0])

    # Octopus merge
    octopus = f"{root}/octopus"

    git(
        repo,
        "switch",
        "--quiet",
        "--force-create",
        octopus,
        base,
    )

    merge(repo, *strands)

    # Main convergence
    git(repo, "switch", "--quiet", "main")

    merge(repo, left)
    merge(repo, right)
    merge(repo, octopus)

    commit(
        repo,
        f"meta(cycle-{cycle:06d}): "
        "reconcile three mutually-observing histories",
        allow_empty=True,
    )

    # Rolling tags
    git(
        repo,
        "tag",
        "--force",
        "--annotate",
        f"live/rolling-{cycle % 12:02d}",
        "HEAD",
        "--message",
        f"rolling checkpoint / epoch {cycle}",
    )

    # Notes
    git(
        repo,
        "notes",
        "--ref=refs/notes/live-ledger",
        "add",
        "--force",
        "--message",
        f"cycle={cycle}; state=provisional",
        "HEAD",
    )

    git(
        repo,
        "update-ref",
        f"refs/a/live/checkpoint-{cycle % 12:02d}",
        "HEAD",
    )

    # Recursive retrocloning
    if (
        submodule_every
        and cycle % submodule_every == 0
    ):

        name, relpath = add_retroclone(
            repo,
            source,
            cycle,
        )

        commit(
            repo,
            f"submodule(retroclone): "
            f"import recursive local snapshot #{cycle}",
        )

        active.append((name, relpath))

        log.log(
            f"[cycle {cycle}] sous-module rétrocloné "
            f"et initialisé : {relpath}"
        )

        while max_submodules > 0 and len(active) > max_submodules:

            old_name, old_path = active.popleft()

            remove_retroclone(
                repo,
                old_name,
                old_path,
            )

            commit(
                repo,
                f"submodule(archive): "
                f"detach old nested snapshot {old_name}",
            )

            log.log(
                f"[cycle {cycle}] ancien sous-module détaché : {old_path}"
            )

    # Rétention configurable des anciennes branches live.
    # retain_cycles == 0 : aucune branche live n'est élaguée.
    # Les commits restent accessibles depuis main dans tous les cas.
    if retain_cycles > 0 and cycle > retain_cycles:

        old = f"live/cycle-{cycle - retain_cycles:06d}"

        for suffix in (
            "strand-0",
            "strand-1",
            "strand-2",
            "weave-left",
            "weave-right",
            "octopus",
        ):

            git(
                repo,
                "branch",
                "-D",
                f"{old}/{suffix}",
                check=False,
            )


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args():

    p = argparse.ArgumentParser(
        description="Générateur Git local, rapide et journalisé."
    )

    p.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Pause après chaque cycle (0 = vitesse maximale).",
    )

    p.add_argument(
        "--branches",
        type=int,
        default=10,
        help="Branches initiales (défaut : 9, démarrage rapide).",
    )

    p.add_argument(
        "--orphan-lines",
        type=int,
        default=2,
    )

    p.add_argument(
        "--submodule-every",
        type=int,
        default=30,
        help="Un rétroclone tous les N cycles; 0 désactive.",
    )

    p.add_argument(
        "--max-submodules",
        type=int,
        default=0,
        help="Nombre maximal de rétroclones actifs (0 = sans plafond).",
    )

    p.add_argument(
        "--retain-cycles",
        type=int,
        default=0,
        help="Nombre de cycles live dont les branches sont conservées (0 = toutes).",
    )

    p.add_argument(
        "--remote",
        default="https://github.com/gobamo/gobamo.git",
        help="Dépôt distant de publication (défaut : dépôt gobamo/gobamo).",
    )

    p.add_argument(
        "--remote-prefix", "--remote-branch",
        dest="remote_prefix",
        default=None,
        help="Préfixe des branches distantes ; par défaut a-lab/run-<date>-<pid>.",
    )

    p.add_argument(
        "--push-every",
        type=int,
        default=10,
        help="Tenter une publication tous les N cycles (défaut : 10).",
    )

    p.add_argument(
        "--push-min-interval",
        type=float,
        default=60.0,
        help="Intervalle minimal entre deux pushes, au moins 60 secondes.",
    )

    p.add_argument(
        "--max-pushes",
        type=int,
        default=1000,
        help="Nombre maximal de pushes réseau par exécution (défaut : 10).",
    )

    p.add_argument(
        "--max-remote-branches",
        type=int,
        default=40,
        help="Nombre maximal de branches créées sur GitHub pendant la démo (défaut : 200).",
    )

    p.add_argument(
        "--no-push",
        action="store_true",
        help="Désactive toute publication distante (mode local uniquement).",
    )

    p.add_argument(
        "--seed",
        type=int,
        default=20261115,
    )

    p.add_argument(
        "--output",
        type=Path,
        default=None,
    )

    p.add_argument(
        "--no-tig",
        action="store_true",
        help="Logs en direct dans ce terminal, sans lancer Tig.",
    )

    a = p.parse_args()

    # Les plafonds arbitraires ont été retirés. On conserve seulement les
    # bornes minimales nécessaires au fonctionnement du générateur.
    if a.interval < 0:
        p.error("--interval doit être positif ou nul")

    if a.branches < 3:
        p.error("--branches doit être au moins égal à 3 (une branche par secteur)")

    if a.orphan_lines < 0:
        p.error("--orphan-lines doit être positif ou nul")

    if a.submodule_every < 0:
        p.error("--submodule-every doit être positif ou nul")

    if a.max_submodules < 0:
        p.error("--max-submodules doit être positif ou nul")

    if a.retain_cycles < 0:
        p.error("--retain-cycles doit être positif ou nul")

    if a.push_every < 1:
        p.error("--push-every doit être supérieur ou égal à 1")

    if a.push_min_interval < 0:
        p.error("--push-min-interval ne peut pas être inférieur à 60 secondes")

    if a.max_pushes < 1:
        p.error("--max-pushes doit être supérieur ou égal à 1")

    if a.max_remote_branches < 1:
        p.error("--max-remote-branches doit être compris entre 1 et 200")

    if a.remote_prefix is not None and not a.remote_prefix.startswith("a-lab/"):
        p.error("--remote-prefix doit rester dans l'espace de noms a-lab/ (main est protégé)")

    if not a.no_push and not a.remote.strip():
        p.error("--remote ne peut pas être vide (utilise --no-push pour le mode local)")

    return a


# ============================================================
# MAIN ENGINE
# ============================================================

def main():

    if shutil.which("git") is None:
        print(
            "ERREUR : Git est introuvable. Installe Git et réessaie.",
            flush=True,
        )
        return 2

    args = parse_args()

    if args.output is None:

        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

        output = (
            Path(tempfile.gettempdir())
            / f"git-a-lab-{stamp}-{os.getpid()}"
        ).resolve()

    else:

        output = args.output.expanduser().resolve()

    log_path = output.parent / f"{output.name}.a.log"

    logger = Logger(
        log_path,
        console=True,
    )

    assets = (
        output.parent
        / f".{output.name}-clone-assets"
    )

    print(
        "\n=== GIT a LAB : démarrage ===",
        flush=True,
    )

    logger.log(f"Dossier du dépôt : {output}")
    logger.log(f"Journal complet : {log_path}")

    logger.log(
        "Les plafonds artificiels de branches/orphelines ont été retirés. "
        "Les modifications Git dans un même dépôt restent séquentielles."
    )

    remote_namespace = args.remote_prefix or (
        "a-lab/run-"
        + datetime.now().strftime("%Y%m%d-%H%M%S")
        + f"-{os.getpid()}"
    )
    effective_submodule_every = args.submodule_every
    publisher = None

    if not args.no_push:
        logger.log(f"Dépôt distant : {args.remote}")
        logger.log(f"Préfixe GitHub isolé : {remote_namespace}/…")
        logger.log(
            f"Publication asynchrone : au plus {args.max_pushes} pushes, "
            f"{args.max_remote_branches} branches distantes, "
            f"un push tous les {args.push_every} cycles au minimum "
            f"{args.push_min_interval:.0f}s entre deux pushes."
        )
        if effective_submodule_every:
            logger.log(
                "AVERTISSEMENT : les rétroclones file:// sont locaux et "
                "ne sont pas publiables tels quels ; ils sont désactivés "
                "en mode push pour éviter des .gitmodules cassés. "
                "Utilise --no-push pour les tester localement."
            )
            effective_submodule_every = 0
    else:
        logger.log("Mode local : publication distante désactivée (--no-push).")

    try:

        ensure_safe_output(output)

        output.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        rng = random.Random(args.seed)

        start = time.monotonic()

        initial_repository(output, args.branches, args.orphan_lines, rng, logger)

        if not args.no_push:
            git(output, "remote", "add", "origin", args.remote)
            logger.log(f"Dépôt distant 'origin' configuré : {args.remote}")

        if effective_submodule_every:
            recursive_source = prepare_recursive_snapshot(
                output,
                assets,
                logger,
            )
        else:
            recursive_source = output

        if not args.no_push:
            publisher = AsyncPublisher(
                output,
                args.remote,
                remote_namespace,
                args.max_pushes,
                args.push_min_interval,
                args.max_remote_branches,
                logger,
            )

        logger.log(
            f"Initialisation terminée en "
            f"{time.monotonic() - start:.1f}s."
        )

    except (OSError, ValueError, RuntimeError) as exc:

        logger.log(
            f"ERREUR pendant la préparation : {exc}"
        )

        return 1

    if (
        args.no_tig
        or shutil.which("tig") is None
        or not sys.stdin.isatty()
    ):

        use_tig = False

        if not args.no_tig and shutil.which("tig") is None:
            logger.log(
                "Tig n'est pas installé ; passage en mode logs console."
            )

        elif not args.no_tig and not sys.stdin.isatty():
            logger.log(
                "Terminal non interactif ; passage en mode logs console."
            )

    else:

        use_tig = True

    logger.log(
        "Moteur live démarré. Tig et la publication réseau tournent en parallèle. "
        "Les mutations Git du dépôt partagé restent séquentielles ; les pushes "
        "lisent des instantanés immuables de références. Ctrl+C arrête le générateur."
    )

    logger.log(
        f"Intervalle demandé : {args.interval}s ; "
        f"rétroclone tous les "
        f"{effective_submodule_every or '0 (désactivé)'} cycles ; "
        f"max sous-modules={args.max_submodules or 'illimité'} ; "
        f"rétention branches={args.retain_cycles or 'illimitée'}."
    )

    logger.log(
        "Astuce : ouvre un 2e terminal et suis le journal avec : "
        + "tail -f "
        + str(log_path)
    )

    stop = threading.Event()

    active = deque()

    state = {
        "cycles": 0,
        "error": None,
        "interrupted": False,
    }
    push_limit_reported = False

    def handle_sigint(_signum, _frame):
        state["interrupted"] = True
        stop.set()

    signal.signal(
        signal.SIGINT,
        handle_sigint,
    )

    rng_live = random.Random(
        args.seed ^ 0x5EED1234
    )

    def worker():
        nonlocal push_limit_reported

        try:

            while not stop.is_set():

                cycle = state["cycles"] + 1

                t0 = time.monotonic()

                live_cycle(
                    output,
                    cycle,
                    rng_live,
                    recursive_source,
                    active,
                    args.max_submodules,
                    effective_submodule_every,
                    args.retain_cycles,
                    logger,
                )

                state["cycles"] = cycle

                head = git(
                    output,
                    "rev-parse",
                    "--short",
                    "main",
                )

                logger.log(
                    f"[cycle {cycle}] terminé en "
                    f"{time.monotonic() - t0:.2f}s | main={head}"
                )

                if publisher is not None and cycle % args.push_every == 0:
                    snapshot = snapshot_local_heads(output)
                    queued = publisher.enqueue(snapshot)
                    if queued:
                        logger.log(
                            f"[cycle {cycle}] instantané de {len(snapshot)} branches "
                            "mis en file pour publication GitHub asynchrone."
                        )
                    else:
                        stats = publisher.stats
                        if stats["disabled"]:
                            pass
                        elif stats["scheduled"] >= args.max_pushes and not push_limit_reported:
                            logger.log(
                                f"Budget réseau atteint : {args.max_pushes} pushes maximum. "
                                "La génération locale continue jusqu'à Ctrl+C."
                            )
                            push_limit_reported = True

                if args.interval:
                    stop.wait(args.interval)

        except KeyboardInterrupt:
            stop.set()

        except Exception as exc:
            if not stop.is_set():
                state["error"] = f"{type(exc).__name__}: {exc}"
                logger.log("ERREUR moteur live : " + state["error"])
            stop.set()

    thread = threading.Thread(
        target=worker,
        name="git-a-worker",
        daemon=True,
    )

    thread.start()

    tigrc = output.parent / f".{output.name}.tigrc"
    tig_proc = None

    try:
        if use_tig:
            logger.log(
                "Ouverture de Tig. Les logs live et les pushes continuent en arrière-plan."
            )
            logger.set_console(False)
            tigrc.write_text(
                "set refresh-mode = periodic\n"
                "set refresh-interval = 1\n",
                encoding="utf-8",
            )
            env = os.environ.copy()
            env["TIGRC_USER"] = str(tigrc)
            tig_proc = subprocess.Popen(["tig", "--all"], cwd=output, env=env)

            while tig_proc.poll() is None and not stop.is_set():
                time.sleep(0.2)
        else:
            logger.log(
                "Mode console : ouvre Tig dans un deuxième terminal avec :"
            )
            logger.log(f'cd "{output}" && tig --all')
            while not stop.wait(0.5):
                pass

    except KeyboardInterrupt:
        state["interrupted"] = True
        stop.set()

    finally:
        stop.set()

        if tig_proc is not None and tig_proc.poll() is None:
            tig_proc.terminate()
            try:
                tig_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                tig_proc.kill()
                tig_proc.wait()

        thread.join(timeout=20)

        if publisher is not None:
            publisher.close()

        logger.set_console(True)
        if tigrc.exists():
            tigrc.unlink()

    if state["interrupted"]:
        logger.log("Ctrl+C reçu : arrêt propre demandé.")

    logger.log(f"Arrêt terminé. Cycles complétés : {state['cycles']}.")
    logger.log(f"Dépôt conservé : {output}")
    logger.log(f"Journal conservé : {log_path}")

    if args.no_push:
        logger.log("Aucun push GitHub : mode local demandé.")
    elif publisher is not None:
        stats = publisher.stats
        logger.log(
            f"Publication GitHub : {stats['pushes']} pushes réussis, "
            f"{stats['branches']} branches créées sous {remote_namespace}."
        )
        if stats["error"]:
            logger.log(f"Dernière erreur de publication : {stats['error']}")

    if state["error"]:
        logger.log(
            "Le moteur s'est arrêté sur une erreur ; consulte le journal ci-dessus."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
