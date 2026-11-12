import pytest

from feather_engine_dbos import system_schema


def test_the_default_tenant_keeps_the_dbos_schema_and_another_has_its_own():  # ADR-0013 rule 7
    assert system_schema() == system_schema("default") == "dbos"
    assert system_schema("acme") == "dbos_acme" and system_schema("Acme_2") == "dbos_acme_2"


@pytest.mark.parametrize("tenant", ["a-b", "a;drop", "", "ünï"])
def test_a_tenant_that_cant_name_a_schema_is_refused(tenant: str):
    with pytest.raises(ValueError, match="can't name a schema"):
        system_schema(tenant)
