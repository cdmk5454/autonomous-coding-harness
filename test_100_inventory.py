import ctypes
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import production_isolation
from production_isolation import inventory
from qa_quarantine import CandidateError


class ParallelInventoryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_full_byte_parity_and_deterministic_order(self):
        for name, data in (("b.txt", b"b"), ("a.txt", b"a"), ("nested/c.bin", b"c" * 4096)):
            path = self.root / name
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(data)
        expected = {name: hashlib.sha256((self.root / name).read_bytes()).hexdigest()
                    for name in ("a.txt", "b.txt", "nested/c.bin")}
        self.assertEqual(expected, inventory(self.root, workers=1))
        self.assertEqual(expected, inventory(self.root, workers=4))
        self.assertEqual(list(expected), list(inventory(self.root, workers=4)))

    def test_same_size_same_mtime_content_change_is_not_cached(self):
        path = self.root / "data.bin"
        path.write_bytes(b"before")
        before = inventory(self.root, workers=2)
        st = path.stat()
        path.write_bytes(b"after!")
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))
        self.assertNotEqual(before, inventory(self.root, workers=2))

    def test_mutation_between_scan_and_open_fails_closed(self):
        path = self.root / "data.bin"
        path.write_bytes(b"before")
        actual_open = production_isolation._open_inventory_fd
        def mutate_then_open(target):
            path.write_bytes(b"after and longer")
            return actual_open(target)
        with patch.object(production_isolation, "_open_inventory_fd", side_effect=mutate_then_open):
            with self.assertRaisesRegex(CandidateError, "CANDIDATE_FILE_CHANGED_DURING_INVENTORY"):
                inventory(self.root, workers=2)

    def test_hardlink_and_symlink_are_denied(self):
        source = self.root / "source"
        source.write_bytes(b"x")
        os.link(source, self.root / "hardlink")
        with self.assertRaisesRegex(CandidateError, "CANDIDATE_HARDLINK_DENIED"):
            inventory(self.root, workers=2)
        (self.root / "hardlink").unlink()
        try:
            os.symlink(source, self.root / "symlink")
        except (OSError, NotImplementedError):
            return
        with self.assertRaisesRegex(CandidateError, "CANDIDATE_LINK_DENIED"):
            inventory(self.root, workers=2)

    @unittest.skipUnless(os.name == "nt", "Windows share-lock behavior")
    def test_locked_included_file_fails_closed(self):
        path = self.root / "locked.bin"
        path.write_bytes(b"x")
        kernel = ctypes.windll.kernel32
        kernel.CreateFileW.restype = ctypes.c_void_p
        handle = kernel.CreateFileW(str(path), 0x80000000, 0, None, 3, 0x80, None)
        self.assertNotEqual(ctypes.c_void_p(-1).value, handle)
        try:
            with self.assertRaisesRegex(CandidateError, "CANDIDATE_FILE_UNREADABLE"):
                inventory(self.root, workers=2)
        finally:
            kernel.CloseHandle(ctypes.c_void_p(handle))


if __name__ == "__main__":
    unittest.main()
