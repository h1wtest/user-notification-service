"""Exercise the real canonical contract checker against changed source inventories."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class ContractsDigestTests(unittest.TestCase):
    def test_changed_contract_source_rejects_previously_verified_tree(self):
        checker = Path(__file__).with_name('contracts-digest.py')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schema = root / 'contracts/schema/events.json'
            schema.parent.mkdir(parents=True)
            schema.write_text('{"type":"object"}\n')
            command = [sys.executable, str(checker), str(root / 'contracts')]
            actual = subprocess.run(command, check=True, capture_output=True, text=True, timeout=15).stdout
            lock = root / 'contracts-tree.sha256'
            lock.write_text(actual)
            subprocess.run([*command, '--verify', str(lock)], check=True, capture_output=True, timeout=15)
            schema.write_text('{"type":"string"}\n')
            result = subprocess.run([*command, '--verify', str(lock)], capture_output=True, text=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('contracts tree digest differs from pinned source', result.stderr)
            self.assertEqual(lock.read_text(), actual)


if __name__ == '__main__':
    unittest.main()
