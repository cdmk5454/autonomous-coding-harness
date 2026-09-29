import ctypes
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from harness_11.core import BoundaryError, Resource, Scope
from harness_11.context import read_source_bytes


@unittest.skipUnless(os.name == "nt", "Windows handle and junction behavior")
class WindowsContextReaderTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.candidate = self.root / "candidate"
        self.canonical = self.root / "canonical"
        (self.candidate / "src").mkdir(parents=True)
        self.canonical.mkdir()
        self.scope = Scope(str(self.candidate), str(self.canonical), ("src",),
                           (Resource("repo/test"),))

    def test_exact_bytes_and_sensitive_name(self):
        (self.candidate / "src" / "a.py").write_bytes(b"safe\n")
        self.assertEqual(b"safe\n", read_source_bytes(self.scope, "src/a.py", 1024))
        (self.candidate / "src" / ".env").write_bytes(b"fixture")
        with self.assertRaisesRegex(BoundaryError, "CONTEXT_SENSITIVE_PATH_DENIED"):
            read_source_bytes(self.scope, "src/.env", 1024)

    def test_junction_to_in_tree_directory_is_denied(self):
        real = self.candidate / "src" / "real"
        real.mkdir()
        (real / "a.py").write_bytes(b"safe")
        alias = self.candidate / "src" / "alias"
        created = subprocess.run(["cmd.exe", "/c", "mklink", "/J", str(alias), str(real)],
                                 capture_output=True)
        self.assertEqual(0, created.returncode)
        with self.assertRaisesRegex(BoundaryError, "SOURCE_PATH_ESCAPE"):
            read_source_bytes(self.scope, "src/alias/a.py", 1024)

    def test_locked_file_fails_closed(self):
        path = self.candidate / "src" / "locked.py"
        path.write_bytes(b"safe")
        kernel = ctypes.windll.kernel32
        kernel.CreateFileW.restype = ctypes.c_void_p
        handle = kernel.CreateFileW(str(path), 0x80000000, 0, None, 3, 0x80, None)
        self.assertNotEqual(ctypes.c_void_p(-1).value, handle)
        try:
            with self.assertRaisesRegex(BoundaryError, "CONTEXT_SECURE_OPEN_FAILED"):
                read_source_bytes(self.scope, "src/locked.py", 1024)
        finally:
            kernel.CloseHandle(ctypes.c_void_p(handle))

    def test_directory_swap_after_path_validation_fails_closed(self):
        import harness_11.context as context
        source = self.candidate/'src'
        displaced = self.candidate/'src-original'
        (source/'a.py').write_bytes(b'safe')
        (self.canonical/'a.py').write_bytes(b'canonical-private')
        original = context.relative_source
        def race(scope,relative):
            clean=original(scope,relative)
            source.rename(displaced)
            created=subprocess.run(['cmd.exe','/c','mklink','/J',str(source),str(self.canonical)],
                capture_output=True)
            self.assertEqual(0,created.returncode)
            return clean
        try:
            with patch.object(context,'relative_source',side_effect=race):
                with self.assertRaises(BoundaryError):
                    read_source_bytes(self.scope,'src/a.py',1024)
        finally:
            if source.exists(): source.rmdir()
            if displaced.exists(): displaced.rename(source)


if __name__ == "__main__":
    unittest.main()
