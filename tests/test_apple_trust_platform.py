import sys
import types
import unittest
from unittest.mock import Mock, patch

from fastcheck.protocols import apple_trust


class AppleTrustPlatformTests(unittest.TestCase):
    def test_linux_ignores_installed_truststore(self):
        stdlib_context = Mock(name="stdlib-context")
        truststore = types.SimpleNamespace(
            SSLContext=Mock(side_effect=AssertionError("truststore used on Linux"))
        )
        with patch.object(apple_trust.sys, "platform", "linux"), \
                patch.object(apple_trust.ssl, "create_default_context", return_value=stdlib_context), \
                patch.dict(sys.modules, {"truststore": truststore}):
            context = apple_trust._new_gsa_context()

        self.assertIs(context, stdlib_context)
        truststore.SSLContext.assert_not_called()

    def test_macos_uses_truststore_when_available(self):
        native_context = Mock(name="native-context")
        truststore = types.SimpleNamespace(SSLContext=Mock(return_value=native_context))
        with patch.object(apple_trust.sys, "platform", "darwin"), \
                patch.dict(sys.modules, {"truststore": truststore}):
            context = apple_trust._new_gsa_context()

        self.assertIs(context, native_context)
        truststore.SSLContext.assert_called_once_with(apple_trust.ssl.PROTOCOL_TLS_CLIENT)


if __name__ == "__main__":
    unittest.main()
