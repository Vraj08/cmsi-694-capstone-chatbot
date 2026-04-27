import unittest
from pathlib import Path
from unittest.mock import patch

from oa_app.integrations import supabase_io


class SupabaseFallbackTests(unittest.TestCase):
    def _write_fallback_secrets(self) -> Path:
        secrets_path = Path(__file__).with_name("_tmp_supabase_secrets.toml")
        secrets_path.write_text(
            'SUPABASE_URL = "https://example.supabase.co"\nSUPABASE_KEY = "fallback-key"\n',
            encoding="utf-8",
        )
        return secrets_path

    def tearDown(self):
        supabase_io._fallback_supabase_values.cache_clear()
        try:
            Path(__file__).with_name("_tmp_supabase_secrets.toml").unlink()
        except (FileNotFoundError, PermissionError):
            pass

    def test_supabase_enabled_uses_fallback_file_when_local_missing(self):
        secrets_path = self._write_fallback_secrets()
        with (
            patch.object(supabase_io, "_FALLBACK_SECRETS_PATH", secrets_path),
            patch.object(supabase_io, "_local_secret", return_value=""),
        ):
            supabase_io._fallback_supabase_values.cache_clear()
            self.assertTrue(supabase_io.supabase_enabled())
            self.assertEqual(
                supabase_io._supabase_credentials(),
                ("https://example.supabase.co", "fallback-key"),
            )

    def test_local_supabase_secrets_override_fallback(self):
        def fake_local(name: str) -> str:
            return {
                "SUPABASE_URL": "https://local.supabase.co",
                "SUPABASE_KEY": "local-key",
            }.get(name, "")

        secrets_path = self._write_fallback_secrets()
        with (
            patch.object(supabase_io, "_FALLBACK_SECRETS_PATH", secrets_path),
            patch.object(supabase_io, "_local_secret", side_effect=fake_local),
        ):
            supabase_io._fallback_supabase_values.cache_clear()
            self.assertEqual(
                supabase_io._supabase_credentials(),
                ("https://local.supabase.co", "local-key"),
            )


if __name__ == "__main__":
    unittest.main()
