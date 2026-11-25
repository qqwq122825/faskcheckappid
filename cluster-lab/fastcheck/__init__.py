"""Vendored authentication core plus cluster executor compatibility modules."""

# unittest.mock and the cluster executor address these as attributes of the
# package.  Import them eagerly so the vendored package behaves like the
# formerly ancestor-discovered package in both production and isolated tests.
from . import proxy_pool, workers  # noqa: F401
