from __future__ import annotations


def _patch_miniracer_del(py_mini_racer):
    for attr in ("MiniRacer", "StrictMiniRacer"):
        cls = getattr(py_mini_racer, attr, None)
        if cls is None:
            continue

        if getattr(cls, "_trae_safe_del", False):
            continue

        orig_del = getattr(cls, "__del__", None)
        if orig_del is None:
            continue

        def _safe_del(self, _orig_del=orig_del):
            try:
                _orig_del(self)
            except Exception:
                pass

        try:
            setattr(cls, "__del__", _safe_del)
            setattr(cls, "_trae_safe_del", True)
        except Exception:
            pass


def ensure_py_mini_racer():
    try:
        import py_mini_racer
    except Exception:
        return

    if getattr(py_mini_racer, "__file__", None) is None:
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

        if not hasattr(py_mini_racer, "MiniRacer"):
            try:
                from py_mini_racer import _mini_racer
            except Exception:
                _mini_racer = None

            if _mini_racer is not None:
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

    _patch_miniracer_del(py_mini_racer)
