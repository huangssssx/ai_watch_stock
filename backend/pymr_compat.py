from __future__ import annotations


def ensure_py_mini_racer():
    try:
        import py_mini_racer
    except Exception:
        return

    if getattr(py_mini_racer, "__file__", None) is not None:
        return

    try:
        spec = getattr(py_mini_racer, "__spec__", None)
        if spec is not None and getattr(spec, "origin", None) is None:
            import pathlib

            pkg_dir = None
            pkg_path = getattr(py_mini_racer, "__path__", None)
            if pkg_path:
                pkg_dir = str(next(iter(pkg_path)))
            if pkg_dir:
                spec.origin = str(pathlib.Path(pkg_dir) / "__init__.py")
    except Exception:
        pass

    if hasattr(py_mini_racer, "MiniRacer"):
        return

    try:
        from py_mini_racer import _mini_racer
    except Exception:
        return

    try:
        py_mini_racer.MiniRacer = _mini_racer.MiniRacer
        py_mini_racer.StrictMiniRacer = getattr(
            _mini_racer, "StrictMiniRacer", _mini_racer.MiniRacer
        )
        py_mini_racer.WrongReturnTypeException = getattr(
            _mini_racer, "WrongReturnTypeException", Exception
        )
        py_mini_racer.__all__ = [
            "MiniRacer",
            "StrictMiniRacer",
            "WrongReturnTypeException",
        ]
    except Exception:
        pass
