import unittest
from unittest.mock import patch

from oa_app.integrations import supabase_io


class SupabaseFallbackTests(unittest.TestCase):
    def test_supabase_disabled_when_local_secrets_missing(self):
        with patch.object(supabase_io, "_local_secret", return_value=""):
            self.assertFalse(supabase_io.supabase_enabled())
            self.assertEqual(supabase_io._supabase_credentials(), ("", ""))

    def test_local_supabase_secrets_enable_supabase(self):
        def fake_local(name: str) -> str:
            return {
                "SUPABASE_URL": "https://local.supabase.co",
                "SUPABASE_KEY": "local-key",
            }.get(name, "")

        with patch.object(supabase_io, "_local_secret", side_effect=fake_local):
            self.assertTrue(supabase_io.supabase_enabled())
            self.assertEqual(
                supabase_io._supabase_credentials(),
                ("https://local.supabase.co", "local-key"),
            )


if __name__ == "__main__":
    unittest.main()
