from pathlib import Path
import shutil
import nox

nox.options.reuse_existing_virtualenvs = True


@nox.session(python=False, default=False)
def clean(session):
    pats = ["dist", "build", "*.egg-info", ".coverage*", ".*cache", ".nox", "*.log", "*.tmp"]
    for w in pats:
        for f in Path.cwd().glob(w):
            session.log(f"Removing: {f}")
            if f.is_dir():
                shutil.rmtree(f, ignore_errors=True)
            else:
                f.unlink(missing_ok=True)
    for f in Path.cwd().rglob("__pycache__"):
        session.log(f"Removing: {f}")
        shutil.rmtree(f, ignore_errors=True)


@nox.session
def tests(session: nox.Session) -> None:
    session.install("-e", ".[test]")
    session.run("python", "-m", "pytest", "tests")


@nox.session
def typecheck(session: nox.Session) -> None:
    session.install("-e", ".[test]", "mypy~=2.1")
    session.run("mypy", *session.posargs)


@nox.session
def black(session: nox.Session) -> None:
    session.install("black~=26.5")
    default = ("--check", "cynic.py", "tests", "noxfile.py")
    session.run("python", "-m", "black", *(session.posargs or default))
